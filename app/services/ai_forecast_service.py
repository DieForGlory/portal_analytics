import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime, timedelta
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from app.models.estate_models import EstateDeal, EstateSell, EstateHouse
from app.core.extensions import db


class AIForecastService:
    MODEL_PATH = 'app/models/ai/sales_forecast_model.pkl'
    FEATURES_PATH = 'app/models/ai/model_features.pkl'
    APARTMENT_KEYWORDS = ['квартир', 'flat', 'apartment', 'жил']

    @staticmethod
    def _get_apartment_filter():
        conditions = [db.func.lower(EstateSell.estate_sell_category).contains(kw) for kw in
                      AIForecastService.APARTMENT_KEYWORDS]
        return db.or_(*conditions)

    @staticmethod
    def train_with_validation():
        # Сбор данных за 3 года для глубокого анализа циклов
        start_date = datetime.now() - timedelta(days=1095)

        data_query = db.session.query(
            EstateHouse.complex_name,
            db.func.extract('month', EstateDeal.agreement_date).label('month'),
            db.func.extract('year', EstateDeal.agreement_date).label('year'),
            db.func.count(EstateDeal.id).label('sales_count')
        ).select_from(EstateDeal) \
            .join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
            .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
            .filter(AIForecastService._get_apartment_filter()) \
            .filter(EstateDeal.agreement_date >= start_date) \
            .group_by('year', 'month', EstateHouse.complex_name).all()

        df = pd.DataFrame([dict(row._asdict()) for row in data_query])
        if df.empty: return "Данные не найдены"

        df['month'] = df['month'].astype(int)
        df['year'] = df['year'].astype(int)
        df = df.sort_values(['complex_name', 'year', 'month'])

        # Feature Engineering: Лаги (1-6 мес) и скользящие средние
        for i in range(1, 7):
            df[f'lag_{i}'] = df.groupby('complex_name')['sales_count'].shift(i).fillna(0)

        df['rolling_mean_3'] = df.groupby('complex_name')['sales_count'].shift(1).rolling(window=3,
                                                                                          min_periods=1).mean().fillna(
            0)
        df['rolling_mean_6'] = df.groupby('complex_name')['sales_count'].shift(1).rolling(window=6,
                                                                                          min_periods=1).mean().fillna(
            0)

        # Динамика всей компании
        company_dynamics = df.groupby(['year', 'month'])['sales_count'].sum().reset_index()
        company_dynamics.columns = ['year', 'month', 'total_company_sales']
        company_dynamics['company_lag_1'] = company_dynamics['total_company_sales'].shift(1).fillna(0)
        df = df.merge(company_dynamics[['year', 'month', 'company_lag_1']], on=['year', 'month'], how='left')

        # Циклическое кодирование времени
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

        df_final = pd.get_dummies(df, columns=['complex_name'])
        df_final = df_final.apply(pd.to_numeric, errors='coerce').fillna(0)

        # Валидация по Ноябрю 2025
        test_mask = (df_final['month'] == 11) & (df_final['year'] == 2025)
        df_train = df_final[~test_mask]
        df_test = df_final[test_mask]

        if df_test.empty: return "Ошибка: данные за 11.2025 отсутствуют"

        X_train = df_train.drop(columns=['sales_count', 'year', 'month'])
        y_train = df_train['sales_count']
        X_test = df_test.drop(columns=['sales_count', 'year', 'month'])
        y_test = df_test['sales_count']

        # Веса: свежие данные (осень 2025) имеют приоритет x25
        weights = np.where((df_train['year'] == 2025) & (df_train['month'] >= 8), 25.0, 1.0)

        model = GradientBoostingRegressor(
            n_estimators=2000,
            learning_rate=0.02,
            max_depth=6,
            min_samples_leaf=2,
            random_state=42,
            loss='absolute_error'  # Устойчивость к аномалиям
        )
        model.fit(X_train, y_train, sample_weight=weights)

        mae = mean_absolute_error(y_test, model.predict(X_test))

        os.makedirs(os.path.dirname(AIForecastService.MODEL_PATH), exist_ok=True)
        joblib.dump(model, AIForecastService.MODEL_PATH)
        joblib.dump(X_train.columns.tolist(), AIForecastService.FEATURES_PATH)

        return round(mae, 4)

    @staticmethod
    def predict_for_month(target_month, target_year=2026):
        if not os.path.exists(AIForecastService.MODEL_PATH):
            return {"error": "Модель не обучена"}

        model = joblib.load(AIForecastService.MODEL_PATH)
        model_features = joblib.load(AIForecastService.FEATURES_PATH)

        # Сбор исторического контекста (12 мес)
        history_start = datetime.now() - timedelta(days=365)
        history_query = db.session.query(
            EstateHouse.complex_name,
            db.func.extract('month', EstateDeal.agreement_date).label('month'),
            db.func.extract('year', EstateDeal.agreement_date).label('year'),
            db.func.count(EstateDeal.id).label('sales_count')
        ).select_from(EstateDeal).join(EstateSell).join(EstateHouse) \
            .filter(EstateDeal.agreement_date >= history_start) \
            .group_by('year', 'month', EstateHouse.complex_name).all()

        h_df = pd.DataFrame([dict(row._asdict()) for row in history_query])

        # ИСПРАВЛЕНИЕ: Используем корректное имя поля estate_price_m2
        inventory_query = db.session.query(
            EstateHouse.complex_name,
            db.func.count(EstateSell.id).label('stock'),
            db.func.avg(EstateSell.estate_price_m2).label('avg_price')
        ).select_from(EstateHouse).join(EstateSell) \
            .filter(AIForecastService._get_apartment_filter()) \
            .filter(EstateSell.estate_sell_status_name.in_(['Подбор', 'Маркетинговый резерв', 'Забронировано', 'Бронь'])) \
            .group_by(EstateHouse.complex_name).all()

        # Общий темп компании за последний месяц
        last_m = datetime.now().month - 1 or 12
        total_recent_sales = h_df[h_df['month'] == last_m]['sales_count'].sum()

        results = []
        print(f"\n=== АНАЛИЗ ПРОГНОЗА: {target_month}/{target_year} ===")

        for row in inventory_query:
            proj, stock, avg_price = row.complex_name, row.stock, float(row.avg_price or 0)
            if stock <= 0: continue

            proj_history = h_df[h_df['complex_name'] == proj].sort_values(['year', 'month'], ascending=False)
            input_df = pd.DataFrame(0.0, index=[0], columns=model_features)

            # 1. Формирование признаков
            last_3_avg = proj_history.head(3)['sales_count'].mean() if not proj_history.empty else 0
            if not proj_history.empty:
                input_df.at[0, 'rolling_mean_3'] = float(last_3_avg)
                input_df.at[0, 'rolling_mean_6'] = float(proj_history.head(6)['sales_count'].mean())
                for i in range(1, 7):
                    if len(proj_history) >= i:
                        input_df.at[0, f'lag_{i}'] = float(proj_history.iloc[i - 1]['sales_count'])

            input_df.at[0, 'company_lag_1'] = float(total_recent_sales)
            input_df.at[0, 'month_sin'] = float(np.sin(2 * np.pi * target_month / 12))
            input_df.at[0, 'month_cos'] = float(np.cos(2 * np.pi * target_month / 12))

            col = f"complex_name_{proj}"
            if col in model_features:
                input_df.at[0, col] = 1.0
                raw_pred = model.predict(input_df)[0]

                # 2. Safety Floor (Предохранитель падения)
                safety_floor = last_3_avg * 0.7
                base_demand = max(raw_pred, safety_floor) if last_3_avg > 15 else raw_pred

                # 3. Liquidity Factor (Затухание при дефиците)
                months_to_exhaust = stock / base_demand if base_demand > 0 else 10

                liquidity_factor = 1.0
                if months_to_exhaust < 2.5:
                    liquidity_factor = 0.4 + (0.6 * (months_to_exhaust / 2.5))

                # 4. Absorption Cap (Лимит вымываемости)
                max_absorption = 0.30
                abs_limit = stock * max_absorption

                # Итоговая коррекция
                final_demand = min(base_demand * liquidity_factor, abs_limit)
                final_result = min(int(round(max(0, final_demand))), stock)

                if last_3_avg > 10:
                    print(
                        f"ЖК: {proj:20} | Факт(3м): {last_3_avg:4.1f} | ИИ: {raw_pred:4.1f} | К.Ликв: {liquidity_factor:.2f} | Итог: {final_result}")

                results.append({
                    "project": proj,
                    "forecast": final_result,
                    "stock": stock,
                    "avg_price": int(avg_price)
                })

        return sorted(results, key=lambda x: x['forecast'], reverse=True)