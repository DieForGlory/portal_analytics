# app/services/project_dashboard_service.py
import pandas as pd
import numpy as np
from datetime import date
from collections import defaultdict
from sqlalchemy import func, extract, case

from app.models import planning_models
from ..core.db_utils import get_planning_session, get_mysql_session
from ..models.estate_models import EstateDeal, EstateHouse, EstateSell
from ..models.finance_models import FinanceOperation
from ..models.planning_models import map_russian_to_mysql_key, map_mysql_key_to_russian_value
from dateutil.relativedelta import relativedelta

# --- ИЗВЛЕЧЕН ИЗ report_service.py ---
def get_price_dynamics_data(complex_name: str, mysql_property_key: str = None):
    """
    Рассчитывает динамику средней фактической цены продажи за м² по месяцам.
    mysql_property_key: Ожидает 'flat', 'comm' и т.д. или None.
    """
    mysql_session = get_mysql_session()

    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)

    query = mysql_session.query(
        extract('year', effective_date).label('deal_year'),
        extract('month', effective_date).label('deal_month'),
        (EstateDeal.deal_sum / EstateSell.estate_area).label('price_per_sqm')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        effective_date.isnot(None),
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"]),
        EstateSell.estate_area.isnot(None),
        EstateSell.estate_area > 0,
        EstateDeal.deal_sum.isnot(None),
        EstateDeal.deal_sum > 0
    )

    if mysql_property_key:
        query = query.filter(EstateSell.estate_sell_category == mysql_property_key)

    subquery = query.subquery()
    monthly_avg_query = mysql_session.query(
        subquery.c.deal_year,
        subquery.c.deal_month,
        func.avg(subquery.c.price_per_sqm).label('avg_price')
    ).group_by(subquery.c.deal_year, subquery.c.deal_month) \
        .order_by(subquery.c.deal_year, subquery.c.deal_month)

    results = monthly_avg_query.all()

    price_dynamics = {
        "labels": [],
        "data": []
    }
    for row in results:
        price_dynamics["labels"].append(f"{int(row.deal_month):02d}.{int(row.deal_year)}")
        price_dynamics["data"].append(float(row.avg_price))

    return price_dynamics


# --- ИЗВЛЕЧЕН ИЗ report_service.py ---
def _get_yearly_fact_metrics_for_complex(year: int, complex_name: str, property_type: str = None):
    mysql_session = get_mysql_session()
    mysql_prop_key = None
    if property_type:
        mysql_prop_key = map_russian_to_mysql_key(property_type)

    house_ids_query = mysql_session.query(EstateHouse.id).filter_by(complex_name=complex_name)
    house_ids = [id[0] for id in house_ids_query.all()]

    if not house_ids:
        return {'volume': [0] * 12, 'income': [0] * 12}

    fact_volume_by_month = [0] * 12
    fact_income_by_month = [0] * 12
    sold_statuses = ["Сделка в работе", "Сделка проведена"]

    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    volume_query = mysql_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        EstateDeal.deal_status_name.in_(sold_statuses),
        extract('year', effective_date) == year
    )
    if mysql_prop_key:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == mysql_prop_key)

    for row in volume_query.group_by('month').all():
        fact_volume_by_month[row.month - 1] = row.total or 0

    income_query = mysql_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        FinanceOperation.status_name == 'Проведено',
        extract('year', FinanceOperation.date_added) == year
    )
    if mysql_prop_key:
        income_query = income_query.filter(EstateSell.estate_sell_category == mysql_prop_key)

    for row in income_query.group_by('month').all():
        fact_income_by_month[row.month - 1] = row.total or 0

    return {'volume': fact_volume_by_month, 'income': fact_income_by_month}


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ТЕМПА ПРОДАЖ ---
def get_sales_pace_kpi(complex_name: str, mysql_property_key: str = None):
    """
    Рассчитывает KPI по темпу продаж (среднее за 3 мес.)
    """
    mysql_session = get_mysql_session()
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)

    sales_query = mysql_session.query(
        extract('year', effective_date).label('year'),
        extract('month', effective_date).label('month'),
        func.count(EstateDeal.id).label('sales_count')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"]),
        effective_date.isnot(None)
    ).group_by('year', 'month').order_by('year', 'month')

    if mysql_property_key:
        sales_query = sales_query.filter(EstateSell.estate_sell_category == mysql_property_key)

    sales_results = sales_query.all()

    if not sales_results:
        return {'current': 0, 'max': 0, 'min': 0, 'quarterly_comparison': {'labels': [], 'data': []}}

    try:
        df = pd.DataFrame(sales_results, columns=['year', 'month', 'sales_count'])
        df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
        monthly_sales = df.set_index('date')['sales_count']

        # Убедимся, что все месяцы присутствуют
        all_months_idx = pd.date_range(start=monthly_sales.index.min(), end=date.today(), freq='MS')
        monthly_sales = monthly_sales.reindex(all_months_idx, fill_value=0)

        # Рассчитываем темп продаж (rolling average)
        sales_pace_series = monthly_sales.rolling(window=3).mean()

        # Убираем NaN в начале, заменяя их 0, для корректного min()
        sales_pace_series = sales_pace_series.fillna(0)

        current_pace = sales_pace_series.iloc[-1] if not sales_pace_series.empty else 0
        max_pace = sales_pace_series.max()
        # Ищем минимальный темп, который не равен 0 (если есть продажи)
        min_pace_non_zero = sales_pace_series[sales_pace_series > 0].min()
        min_pace = min_pace_non_zero if not pd.isna(min_pace_non_zero) else 0

        # Сравнение по кварталам
        current_year = date.today().year
        quarterly_pace = sales_pace_series[sales_pace_series.index.year == current_year].resample('Q').mean()

        quarter_labels = [f"Q{q}" for q in quarterly_pace.index.quarter]
        quarter_values = [round(v, 1) for v in quarterly_pace.values]

        sales_pace_kpi = {
            'current': round(current_pace, 1),
            'max': round(max_pace, 1),
            'min': round(min_pace, 1),
            'quarterly_comparison': {
                'labels': quarter_labels,
                'data': quarter_values
            }
        }
    except Exception as e:
        print(f"Ошибка при расчете темпа продаж: {e}")
        sales_pace_kpi = {'current': 0, 'max': 0, 'min': 0, 'quarterly_comparison': {'labels': [], 'data': []}}

    return sales_pace_kpi


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ТИПОВ ОПЛАТЫ ---
def get_payment_type_distribution(complex_name: str, mysql_property_key: str = None):
    """
    Рассчитывает распределение сделок по 'deal_program_name' из EstateDeal.
    """
    mysql_session = get_mysql_session()

    payment_type_query = mysql_session.query(
        EstateDeal.deal_program_name,  # <--- ВОЗВРАЩАЕМ КАК ВЫ И ХОТЕЛИ
        func.count(EstateDeal.id).label('deal_count')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"])
    )

    if mysql_property_key:
        payment_type_query = payment_type_query.filter(EstateSell.estate_sell_category == mysql_property_key)

    # Группируем по правильному полю
    payment_type_data = payment_type_query.group_by(EstateDeal.deal_program_name).order_by(
        func.count(EstateDeal.id).desc()).all()

    payment_chart_data = {
        'labels': [row.deal_program_name if row.deal_program_name else "Не указано" for row in payment_type_data],
        'data': [row.deal_count for row in payment_type_data]
    }
    return payment_chart_data


# --- ГЛАВНАЯ ФУНКЦИЯ (ОБНОВЛЕННАЯ) ---
def get_project_dashboard_data(complex_name: str, property_type: str = None):
    """
    property_type: Ожидается русское название (напр. 'Квартира') или None.
    """
    today = date.today()
    mysql_session = get_mysql_session()
    planning_session = get_planning_session()
    sold_statuses = ["Сделка в работе", "Сделка проведена"]
    VALID_STATUSES = ["Маркетинговый резерв", "Подбор"]

    mysql_prop_key = None
    if property_type:
        mysql_prop_key = map_russian_to_mysql_key(property_type)

    houses_in_complex_query = mysql_session.query(EstateHouse).filter_by(complex_name=complex_name).order_by(
        EstateHouse.name)
    houses_in_complex = houses_in_complex_query.all()

    # Получаем ID всех домов в комплексе
    house_ids = [h.id for h in houses_in_complex]

    if not house_ids:
        # Если домов нет, возвращаем пустые данные
        return None

    houses_data = []

    active_version = planning_session.query(planning_models.DiscountVersion).filter_by(is_active=True).first()

    remainders_for_chart = defaultdict(lambda: {'total_price': 0, 'count': 0})

    for house in houses_in_complex:
        house_details = {
            "house_name": house.name,
            "property_types_data": {}
        }

        for prop_type_enum in planning_models.PropertyType:
            prop_type_value = prop_type_enum.value
            mysql_key = map_russian_to_mysql_key(prop_type_value)

            total_units = mysql_session.query(func.count(EstateSell.id)).filter(
                EstateSell.house_id == house.id,
                EstateSell.estate_sell_category == mysql_key
            ).scalar()

            if total_units == 0:
                continue

            sold_units_query = mysql_session.query(func.count(EstateDeal.id)).join(EstateSell).filter(
                EstateSell.house_id == house.id,
                EstateSell.estate_sell_category == mysql_key,
                EstateDeal.deal_status_name.in_(sold_statuses)
            )
            sold_units = sold_units_query.scalar()

            remaining_count = total_units - sold_units
            avg_price_per_sqm = 0
            if remaining_count > 0:
                total_discount_rate = 0
                if active_version:
                    discount = planning_session.query(planning_models.Discount).filter_by(
                        version_id=active_version.id, complex_name=complex_name,
                        property_type=prop_type_enum, payment_method=planning_models.PaymentMethod.FULL_PAYMENT
                    ).first()
                    if discount:
                        total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0) + (
                                    discount.action or 0)

                unsold_units = mysql_session.query(EstateSell).filter(
                    EstateSell.house_id == house.id,
                    EstateSell.estate_sell_category == mysql_key,
                    EstateSell.estate_sell_status_name.in_(VALID_STATUSES)
                ).all()

                prices_per_sqm_list = []
                deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0

                for sell in unsold_units:
                    if sell.estate_price and sell.estate_price > deduction_amount and sell.estate_area and sell.estate_area > 0:
                        price_after_deduction = sell.estate_price - deduction_amount
                        final_price = price_after_deduction * (1 - total_discount_rate)
                        price_per_sqm = final_price / sell.estate_area
                        prices_per_sqm_list.append(price_per_sqm)

                        # Собираем данные для KPI "Стоимость остатков"
                        if (not property_type) or (property_type == prop_type_value):
                            remainders_for_chart[prop_type_value]['total_price'] += final_price
                            remainders_for_chart[prop_type_value]['count'] += 1

                if prices_per_sqm_list:
                    avg_price_per_sqm = sum(prices_per_sqm_list) / len(prices_per_sqm_list)

            house_details["property_types_data"][prop_type_value] = {
                "total_count": total_units,
                "remaining_count": remaining_count,
                "avg_price_per_sqm": avg_price_per_sqm
            }

        if house_details["property_types_data"]:
            houses_data.append(house_details)

    # --- KPI КАРТОЧКИ ---
    remainders_by_type = {}
    for k, v in remainders_for_chart.items():
        if v['count'] > 0:
            remainders_by_type[k] = {
                'total_price': float(v['total_price']),
                'count': int(v['count'])
            }

    volume_query = mysql_session.query(func.sum(EstateDeal.deal_sum)).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        EstateDeal.deal_status_name.in_(sold_statuses)
    )
    if mysql_prop_key:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    total_deals_volume = volume_query.scalar() or 0

    income_query = mysql_session.query(func.sum(FinanceOperation.summa)).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        FinanceOperation.status_name == 'Проведено'
    )
    if mysql_prop_key:
        income_query = income_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    total_income = income_query.scalar() or 0

    # --- ДАННЫЕ ДЛЯ ГРАФИКОВ ---

    # 1. План-факт
    yearly_plan_fact = {
        'labels': [f"{i:02}" for i in range(1, 13)],
        'plan_volume': [0] * 12, 'fact_volume': [0] * 12,
        'plan_income': [0] * 12, 'fact_income': [0] * 12
    }
    plans_query = planning_session.query(planning_models.SalesPlan).filter_by(complex_name=complex_name,
                                                                              year=today.year)
    if property_type:
        plans_query = plans_query.filter_by(property_type=property_type)
    for p in plans_query.all():
        yearly_plan_fact['plan_volume'][p.month - 1] += p.plan_volume
        yearly_plan_fact['plan_income'][p.month - 1] += p.plan_income

    fact_metrics = _get_yearly_fact_metrics_for_complex(today.year, complex_name, property_type)
    yearly_plan_fact['fact_volume'] = fact_metrics['volume']
    yearly_plan_fact['fact_income'] = fact_metrics['income']

    # 2. Недавние сделки
    deals_query = mysql_session.query(
        EstateDeal.id, EstateDeal.deal_sum, EstateSell.estate_sell_category.label('mysql_key'),
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).label('deal_date')
    ).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        EstateDeal.deal_status_name.in_(sold_statuses)
    )
    if mysql_prop_key:
        deals_query = deals_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    recent_deals_raw = deals_query.order_by(
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).desc()
    ).limit(15).all()
    recent_deals = [{
        'id': deal.id, 'deal_sum': deal.deal_sum,
        'property_type': map_mysql_key_to_russian_value(deal.mysql_key),
        'deal_date': deal.deal_date
    } for deal in recent_deals_raw]

    # 3. Анализ спроса (по этажам, комнатам, площадям)
    sales_analysis = {"by_floor": {}, "by_rooms": {}, "by_area": {}}
    type_to_analyze_russian = property_type if property_type else 'Квартира'
    type_to_analyze_mysql = map_russian_to_mysql_key(type_to_analyze_russian)

    if type_to_analyze_russian == 'Квартира':
        base_query = mysql_session.query(EstateSell).join(EstateDeal).filter(
            EstateSell.house_id.in_(house_ids),
            EstateDeal.deal_status_name.in_(sold_statuses),
            EstateSell.estate_sell_category == type_to_analyze_mysql
        )
        floor_data = base_query.with_entities(EstateSell.estate_floor, func.count(EstateSell.id)).group_by(
            EstateSell.estate_floor).order_by(EstateSell.estate_floor).all()
        if floor_data:
            sales_analysis['by_floor']['labels'] = [f"{row[0]} этаж" for row in floor_data if row[0] is not None]
            sales_analysis['by_floor']['data'] = [row[1] for row in floor_data if row[0] is not None]
        rooms_data = base_query.filter(EstateSell.estate_rooms.isnot(None)).with_entities(EstateSell.estate_rooms,
                                                                                          func.count(
                                                                                              EstateSell.id)).group_by(
            EstateSell.estate_rooms).order_by(EstateSell.estate_rooms).all()
        if rooms_data:
            sales_analysis['by_rooms']['labels'] = [f"{int(row[0])}-комн." for row in rooms_data if row[0] is not None]
            sales_analysis['by_rooms']['data'] = [row[1] for row in rooms_data if row[0] is not None]
        area_case = case(
            (EstateSell.estate_area < 40, "до 40 м²"), (EstateSell.estate_area.between(40, 50), "40-50 м²"),
            (EstateSell.estate_area.between(50, 60), "50-60 м²"), (EstateSell.estate_area.between(60, 75), "60-75 м²"),
            (EstateSell.estate_area.between(75, 90), "75-90 м²"), (EstateSell.estate_area >= 90, "90+ м²"),
        )
        area_data = base_query.filter(EstateSell.estate_area.isnot(None)).with_entities(area_case, func.count(
            EstateSell.id)).group_by(area_case).order_by(area_case).all()
        if area_data:
            sales_analysis['by_area']['labels'] = [row[0] for row in area_data if row[0] is not None]
            sales_analysis['by_area']['data'] = [row[1] for row in area_data if row[0] is not None]

    # --- СБОРКА ИТОГОВОГО СЛОВАРЯ ---
    dashboard_data = {
        "complex_name": complex_name,
        "kpi": {
            "total_deals_volume": total_deals_volume,
            "total_income": total_income,
            "remainders_by_type": remainders_by_type
        },
        "charts": {
            "plan_fact_dynamics_yearly": yearly_plan_fact,
            "sales_analysis": sales_analysis,
            "price_dynamics": get_price_dynamics_data(complex_name, mysql_prop_key),

            # --- НОВЫЕ ДАННЫЕ ---
            "payment_type_distribution": get_payment_type_distribution(complex_name, mysql_prop_key),
            "sales_pace_kpi": get_sales_pace_kpi(complex_name, mysql_prop_key)
        },
        "recent_deals": recent_deals,
        "houses_data": houses_data,
    }

    mysql_session.close()
    planning_session.close()

    return dashboard_data


def get_project_passport_data(complex_name: str):
    """
    Собирает все статические и динамические данные для "Паспорта проекта".
    """
    mysql_session = get_mysql_session()
    planning_session = get_planning_session()

    sold_statuses = ["Сделка в работе", "Сделка проведена"]
    remainder_statuses = ["Маркетинговый резерв", "Подбор"]

    # 1. Получаем ID домов в комплексе
    house_ids_query = mysql_session.query(EstateHouse.id).filter(EstateHouse.complex_name == complex_name)
    house_ids = [h[0] for h in house_ids_query.all()]
    if not house_ids:
        return None  # Комплекс не найден в MySQL

    # 2. Получаем статические данные из planning.db
    passport_data = planning_session.query(planning_models.ProjectPassport).get(complex_name)
    if not passport_data:
        # Создаем пустую запись, если ее нет
        passport_data = planning_models.ProjectPassport(complex_name=complex_name)
        planning_session.add(passport_data)
        planning_session.commit()

    # 3. Получаем динамические данные

    # Общее кол-во квартир (всех типов)
    total_units = mysql_session.query(func.count(EstateSell.id)).filter(
        EstateSell.house_id.in_(house_ids)
    ).scalar()

    # Остатки на сегодня (по типам)
    active_version = planning_session.query(planning_models.DiscountVersion).filter_by(is_active=True).first()
    remainders_by_type = defaultdict(lambda: {'count': 0, 'total_price': 0.0, 'total_area': 0.0})
    total_remainders_count = 0

    if active_version:
        for prop_type_enum in planning_models.PropertyType:
            prop_type_value = prop_type_enum.value
            mysql_key = map_russian_to_mysql_key(prop_type_value)

            discount = planning_session.query(planning_models.Discount).filter_by(
                version_id=active_version.id, complex_name=complex_name,
                property_type=prop_type_enum, payment_method=planning_models.PaymentMethod.FULL_PAYMENT
            ).first()

            total_discount_rate = 0
            if discount:
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0) + (
                            discount.action or 0)

            deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0

            remainder_sells_query = mysql_session.query(EstateSell).filter(
                EstateSell.house_id.in_(house_ids),
                EstateSell.estate_sell_category == mysql_key,
                EstateSell.estate_sell_status_name.in_(remainder_statuses),
                EstateSell.estate_price.isnot(None),
                EstateSell.estate_area.isnot(None),
                EstateSell.estate_area > 0
            )

            for sell in remainder_sells_query.all():
                final_price = 0
                if sell.estate_price and sell.estate_price > deduction_amount:
                    final_price = (sell.estate_price - deduction_amount) * (1 - total_discount_rate)

                remainders_by_type[prop_type_value]['count'] += 1
                remainders_by_type[prop_type_value]['total_price'] += final_price
                remainders_by_type[prop_type_value]['total_area'] += sell.estate_area
                total_remainders_count += 1

    # Вычисляем среднюю цену дна
    for prop_type, data in remainders_by_type.items():
        if data['total_area'] > 0:
            data['avg_price_sqm'] = data['total_price'] / data['total_area']
        else:
            data['avg_price_sqm'] = 0

    # Кол-во мес до кадастра (берем для квартир)
    months_to_cadastre = None
    if active_version:
        cadastre_date_q = planning_session.query(planning_models.Discount.cadastre_date).filter(
            planning_models.Discount.version_id == active_version.id,
            planning_models.Discount.complex_name == complex_name,
            planning_models.Discount.property_type == planning_models.PropertyType.FLAT
        ).first()
        if cadastre_date_q and cadastre_date_q[0]:
            delta = relativedelta(cadastre_date_q[0], date.today())
            months_to_cadastre = delta.years * 12 + delta.months

    # Темп продаж (за все время)
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    all_sales_q = mysql_session.query(
        func.count(EstateDeal.id),
        func.min(effective_date),
        func.max(effective_date)
    ).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        EstateDeal.deal_status_name.in_(sold_statuses),
        effective_date.isnot(None)
    )

    total_sales, first_sale_date, last_sale_date = all_sales_q.one()

    all_time_pace = 0
    # Добавляем проверку, что все три значения получены
    if total_sales and first_sale_date and last_sale_date:

        # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
        # Конвертируем строки в date объекты, если они пришли как строки
        if isinstance(first_sale_date, str):
            first_sale_date = date.fromisoformat(first_sale_date)
        if isinstance(last_sale_date, str):
            last_sale_date = date.fromisoformat(last_sale_date)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        months_of_sales = (last_sale_date - first_sale_date).days / 30.44
        if months_of_sales < 1:
            months_of_sales = 1  # Избегаем деления на ноль, если продажи были только в 1 день
        all_time_pace = total_sales / months_of_sales

    # Прогноз даты завершения продаж
    forecast_date = None
    if all_time_pace > 0:
        pace_with_coeff = all_time_pace * 0.8  # Понижающий коэффициент 20%
        if pace_with_coeff > 0:
            months_to_sell = total_remainders_count / pace_with_coeff
            forecast_date = date.today() + relativedelta(months=int(months_to_sell))

    # Наиболее частый вид оплаты
    payment_dist = get_payment_type_distribution(complex_name, None)  # Используем существующую функцию
    top_payment_type = None
    if payment_dist['labels']:
        total_payment_deals = sum(payment_dist['data'])
        top_payment_type = {
            'name': payment_dist['labels'][0],
            'percentage': (payment_dist['data'][0] / total_payment_deals) * 100 if total_payment_deals > 0 else 0
        }

    # Действующие скидки
    active_discounts = []
    if active_version:
        discounts_q = planning_session.query(planning_models.Discount).filter(
            planning_models.Discount.version_id == active_version.id,
            planning_models.Discount.complex_name == complex_name
        ).order_by(planning_models.Discount.property_type, planning_models.Discount.payment_method).all()

        for d in discounts_q:
            active_discounts.append({
                'property_type': d.property_type.value,
                'payment_method': d.payment_method.value,
                'mpp': d.mpp or 0, 'rop': d.rop or 0, 'kd': d.kd or 0, 'action': d.action or 0
            })

    # Полная сумма ожидаемых оплат
    expected_payments = mysql_session.query(func.sum(FinanceOperation.summa)).join(EstateSell).filter(
        EstateSell.house_id.in_(house_ids),
        FinanceOperation.status_name == "К оплате",
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки"
    ).scalar() or 0.0

    # Собираем все в один словарь
    dynamic_data = {
        'total_units': total_units,
        'months_to_cadastre': months_to_cadastre,
        'all_time_sales_pace': all_time_pace,
        'remainders_by_type': dict(remainders_by_type),
        'total_remainders_count': total_remainders_count,
        'top_payment_type': top_payment_type,
        'forecast_date': forecast_date.isoformat() if forecast_date else None,
        'active_discounts': active_discounts,
        'expected_payments': expected_payments
    }

    mysql_session.close()
    planning_session.close()

    return {
        'complex_name': complex_name,
        'static_data': passport_data.to_dict(),
        'dynamic_data': dynamic_data
    }