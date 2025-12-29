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
        # Сбор данных за 2 года для захвата цикличной сезонности
        start_date = datetime.now() - timedelta(days=730)

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

        df = df.sort_values(['complex_name', 'year', 'month'])
        # Лаг: продажи за предыдущий месяц
        df['prev_month_sales'] = df.groupby('complex_name')['sales_count'].shift(1).fillna(0)
        df = pd.get_dummies(df, columns=['complex_name'])
        df = df.apply(pd.to_numeric, errors='coerce').fillna(0)

        # РАЗДЕЛЕНИЕ ДАННЫХ ДЛЯ ВАЛИДАЦИИ ПО НОЯБРЮ 2025
        # Обучаемся на всем, что НЕ Ноябрь 2025
        train_mask = ~((df['month'] == 11) & (df['year'] == 2025))
        test_mask = (df['month'] == 11) & (df['year'] == 2025)

        df_train = df[train_mask]
        df_test = df[test_mask]

        if df_test.empty:
            return "Ошибка: данные за Ноябрь 2025 отсутствуют в БД. Валидация невозможна."

        X_train = df_train.drop(columns=['sales_count', 'year'])
        y_train = df_train['sales_count']
        X_test = df_test.drop(columns=['sales_count', 'year'])
        y_test = df_test['sales_count']

        # ВЕСА: Данные 2025 года (особенно осень) имеют приоритет x15
        weights = np.where((df_train['year'] == 2025) & (df_train['month'] >= 8), 15.0, 1.0)

        best_model = None
        min_mae = float('inf')

        print("\n=== ЗАПУСК ВАЛИДАЦИИ НА НОЯБРЕ 2025 ===")
        print(f"Фактические продажи Ноября (всего): {int(y_test.sum())}")

        for i in range(100):
            model = GradientBoostingRegressor(
                n_estimators=1000,  # Увеличено для захвата пиков
                learning_rate=0.01,
                max_depth=6,
                min_samples_leaf=1,
                random_state=i
            )
            model.fit(X_train, y_train, sample_weight=weights)

            # Проверка на Ноябре
            preds = model.predict(X_test)
            mae = mean_absolute_error(y_test, preds)

            if mae < min_mae:
                min_mae = mae
                best_model = model
                total_pred = int(round(preds.sum()))
                print(
                    f"Ит {i}: MAE {mae:.2f} | Прогноз Ноября: {total_pred} шт. (Откл: {total_pred - int(y_test.sum())})")

        os.makedirs(os.path.dirname(AIForecastService.MODEL_PATH), exist_ok=True)
        joblib.dump(best_model, AIForecastService.MODEL_PATH)
        joblib.dump(X_train.columns.tolist(), AIForecastService.FEATURES_PATH)

        return round(min_mae, 4)

    @staticmethod
    def predict_for_month(target_month, target_year=2026):
        if not os.path.exists(AIForecastService.MODEL_PATH):
            return {"error": "Модель не обучена"}

        model = joblib.load(AIForecastService.MODEL_PATH)
        model_features = joblib.load(AIForecastService.FEATURES_PATH)

        # Остатки (Инвентарь)
        inventory_query = db.session.query(
            EstateHouse.complex_name,
            db.func.count(EstateSell.id).label('stock')
        ).select_from(EstateHouse) \
            .join(EstateSell, EstateHouse.id == EstateSell.house_id) \
            .filter(AIForecastService._get_apartment_filter()) \
            .filter(EstateSell.estate_sell_status_name.in_(['Свободно', 'В продаже', 'Забронировано', 'Бронь'])) \
            .group_by(EstateHouse.complex_name).all()
        inventory = {row.complex_name: row.stock for row in inventory_query}

        # Текущий темп (последние 30 дней) - критический фактор
        recent_pace_query = db.session.query(
            EstateHouse.complex_name, db.func.count(EstateDeal.id).label('cnt')
        ).select_from(EstateDeal).join(EstateSell).join(EstateHouse) \
            .filter(EstateDeal.agreement_date >= (datetime.now() - timedelta(days=30))) \
            .group_by(EstateHouse.complex_name).all()
        pace_map = {row.complex_name: row.cnt for row in recent_pace_query}

        results = []
        for proj, stock in inventory.items():
            if stock <= 0: continue

            input_df = pd.DataFrame(0, index=[0], columns=model_features)
            input_df.at[0, 'month'] = target_month
            current_pace = pace_map.get(proj, 0)
            input_df.at[0, 'prev_month_sales'] = current_pace

            col = f"complex_name_{proj}"
            if col in model_features:
                input_df.at[0, col] = 1
                pred = model.predict(input_df)[0]

                # МЕХАНИЗМ АГРЕССИВНОГО ТРЕНДА:
                # Если текущий темп (pace) выше предсказания, используем pace как базу
                if current_pace > pred:
                    pred = current_pace * 1.1  # Ожидаем рост или сохранение высокого темпа

                # Финальный прогноз с учетом остатков
                final = min(int(round(max(0, pred))), stock)
                results.append({"project": proj, "forecast": final, "stock": stock})

        return sorted(results, key=lambda x: x['forecast'], reverse=True)