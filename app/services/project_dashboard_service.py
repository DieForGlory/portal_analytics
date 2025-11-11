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


# --- ИЗВЛЕЧЕН ИЗ report_service.py ---
def get_price_dynamics_data(complex_name: str, mysql_property_key: str = None):
    """
    Рассчитывает динамику средней фактической цены продажи за м² по месяцам.
    mysql_property_key: Ожидает 'flat', 'comm' и т.д. или None.
    """
    mysql_session = get_mysql_session()

    print(f"\n--- [DEBUG] Вызов get_price_dynamics_data ---")
    print(f"[DEBUG] complex_name: '{complex_name}'")
    print(f"[DEBUG] mysql_property_key: '{mysql_property_key}'")

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

    print(f"[DEBUG] Сгенерированный SQL: {monthly_avg_query.statement.compile(compile_kwargs={'literal_binds': True})}")

    results = monthly_avg_query.all()

    print(f"[DEBUG] Результат из БД (сырой): {results}")
    print(f"[DEBUG] Найдено строк: {len(results)}")
    print(f"--- [DEBUG] Конец get_price_dynamics_data ---\n")

    price_dynamics = {
        "labels": [],
        "data": []
    }
    for row in results:
        price_dynamics["labels"].append(f"{int(row.deal_month):02d}.{int(row.deal_year)}")
        # ИСПРАВЛЕНИЕ: Явно преобразуем Decimal в float
        price_dynamics["data"].append(float(row.avg_price))

    return price_dynamics


# --- ИЗВЛЕЧЕН ИЗ report_service.py ---
def _get_yearly_fact_metrics_for_complex(year: int, complex_name: str, property_type: str = None):
    # (Код этой функции без изменений)
    mysql_session = get_mysql_session()

    # --- ИСПРАВЛЕНИЕ: 'property_type' здесь русское, нужен ключ MySQL ---
    mysql_prop_key = None
    if property_type:
        mysql_prop_key = map_russian_to_mysql_key(property_type)
    # ---

    house = mysql_session.query(EstateHouse).filter_by(complex_name=complex_name).first()
    if not house:
        return {'volume': [0] * 12, 'income': [0] * 12}

    fact_volume_by_month = [0] * 12
    fact_income_by_month = [0] * 12
    sold_statuses = ["Сделка в работе", "Сделка проведена"]

    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    volume_query = mysql_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id == house.id,
        EstateDeal.deal_status_name.in_(sold_statuses),
        extract('year', effective_date) == year
    )
    # --- ИСПРАВЛЕНИЕ: Фильтр по mysql_prop_key ---
    if mysql_prop_key:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    # ---

    for row in volume_query.group_by('month').all():
        fact_volume_by_month[row.month - 1] = row.total or 0

    income_query = mysql_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id == house.id,
        FinanceOperation.status_name == 'Проведено',
        extract('year', FinanceOperation.date_added) == year
    )
    # --- ИСПРАВЛЕНИЕ: Фильтр по mysql_prop_key ---
    if mysql_prop_key:
        income_query = income_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    # ---

    for row in income_query.group_by('month').all():
        fact_income_by_month[row.month - 1] = row.total or 0

    return {'volume': fact_volume_by_month, 'income': fact_income_by_month}


# --- ИЗВЛЕЧЕН ИЗ report_service.py ---
def get_project_dashboard_data(complex_name: str, property_type: str = None):
    """
    property_type: Ожидается русское название (напр. 'Квартира') или None.
    """
    today = date.today()
    mysql_session = get_mysql_session()
    planning_session = get_planning_session()
    sold_statuses = ["Сделка в работе", "Сделка проведена"]
    VALID_STATUSES = ["Маркетинговый резерв", "Подбор"]  # <-- Добавлено

    mysql_prop_key = None
    if property_type:
        mysql_prop_key = map_russian_to_mysql_key(property_type)

    houses_in_complex = mysql_session.query(EstateHouse).filter_by(complex_name=complex_name).order_by(
        EstateHouse.name).all()
    houses_data = []

    active_version = planning_session.query(planning_models.DiscountVersion).filter_by(is_active=True).first()

    # --- ДОБАВЛЕНО: Новая структура для детализации остатков по комнатам ---
    remainder_details = defaultdict(lambda: defaultdict(lambda: {
        'count': 0, 'total_price': 0, 'total_area': 0.0
    }))
    remainders_for_chart = defaultdict(lambda: {'total_price': 0, 'count': 0})
    # ------------------------------------------------------------------------

    for house in houses_in_complex:
        house_details = {
            "house_name": house.name,
            "property_types_data": {}
        }

        for prop_type_enum in planning_models.PropertyType:
            prop_type_value = prop_type_enum.value  # Русское название, напр. 'Квартира'
            mysql_key = map_russian_to_mysql_key(prop_type_value)  # Ключ MySQL, напр. 'flat'

            total_units = mysql_session.query(func.count(EstateSell.id)).filter(
                EstateSell.house_id == house.id,
                EstateSell.estate_sell_category == mysql_key
            ).scalar()

            if total_units == 0:
                continue

            sold_units = mysql_session.query(func.count(EstateDeal.id)).join(EstateSell).filter(
                EstateSell.house_id == house.id,
                EstateSell.estate_sell_category == mysql_key,
                EstateDeal.deal_status_name.in_(sold_statuses)
            ).scalar()

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
                        total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0)

                unsold_units = mysql_session.query(EstateSell).filter(
                    EstateSell.house_id == house.id,
                    EstateSell.estate_sell_category == mysql_key,
                    EstateSell.estate_sell_status_name.in_(VALID_STATUSES)  # <-- Исправлено
                ).all()

                prices_per_sqm_list = []
                deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0

                for sell in unsold_units:
                    if sell.estate_price and sell.estate_price > deduction_amount and sell.estate_area and sell.estate_area > 0:
                        price_after_deduction = sell.estate_price - deduction_amount
                        final_price = price_after_deduction * (1 - total_discount_rate)
                        price_per_sqm = final_price / sell.estate_area
                        prices_per_sqm_list.append(price_per_sqm)

                if prices_per_sqm_list:
                    avg_price_per_sqm = sum(prices_per_sqm_list) / len(prices_per_sqm_list)

            house_details["property_types_data"][prop_type_value] = {
                "total_count": total_units,
                "remaining_count": remaining_count,
                "avg_price_per_sqm": avg_price_per_sqm
            }

        if house_details["property_types_data"]:
            houses_data.append(house_details)

    # --- ИСПРАВЛЕНИЕ: Блок расчета остатков для всей вкладки "Структура остатков" ---
    if active_version:
        for prop_type_enum in planning_models.PropertyType:
            prop_type_value = prop_type_enum.value
            mysql_key = map_russian_to_mysql_key(prop_type_value)

            total_discount_rate = 0
            discount = planning_session.query(planning_models.Discount).filter_by(
                version_id=active_version.id,
                complex_name=complex_name,
                property_type=prop_type_enum,
                payment_method=planning_models.PaymentMethod.FULL_PAYMENT
            ).first()
            if discount:
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0) + (
                            discount.action or 0)

            remainder_sells_query = mysql_session.query(EstateSell).join(EstateHouse).filter(
                EstateHouse.complex_name == complex_name,
                EstateSell.estate_sell_category == mysql_key,
                EstateSell.estate_sell_status_name.in_(VALID_STATUSES)  # <-- Исправлено
            )

            deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0

            if property_type and prop_type_value != property_type:
                continue

            for sell in remainder_sells_query.all():
                if sell.estate_price and sell.estate_price > deduction_amount and sell.estate_area and sell.estate_area > 0 and sell.estate_rooms is not None:
                    price_after_deduction = sell.estate_price - deduction_amount
                    final_price = price_after_deduction * (1 - total_discount_rate)

                    rooms_key = str(int(sell.estate_rooms)) if sell.estate_rooms else '0'

                    remainders_for_chart[prop_type_value]['total_price'] += final_price
                    remainders_for_chart[prop_type_value]['count'] += 1

                    room_entry = remainder_details[prop_type_value][rooms_key]
                    room_entry['count'] += 1
                    room_entry['total_price'] += final_price
                    room_entry['total_area'] += sell.estate_area

        # Вычисляем среднюю цену за м² для детальной структуры
        for prop_type_value in list(remainder_details.keys()):
            for rooms_key, data in list(remainder_details[prop_type_value].items()):
                if data['total_area'] > 0:
                    data['avg_price_sqm'] = float(data['total_price']) / float(data['total_area'])
                else:
                    data['avg_price_sqm'] = 0.0

        # --- FIX: Convert defaultdicts to standard dicts and ensure clean types ---

        # 1. Cleaning remainders_for_chart (KPI summary)
        remainders_by_type = {}
        for k, v in remainders_for_chart.items():
            if v['count'] > 0:
                remainders_by_type[k] = {
                    'total_price': float(v['total_price']),
                    'count': int(v['count'])
                }

        # 2. Cleaning remainder_details (Accordion data)
        final_remainder_details = defaultdict(dict)
        for prop_type, rooms_data in remainder_details.items():
            for rooms_key, data in rooms_data.items():
                final_remainder_details[prop_type][rooms_key] = {
                    'count': int(data['count']),
                    'total_price': float(data['total_price']),
                    'total_area': float(data['total_area']),
                    'avg_price_sqm': float(data.get('avg_price_sqm', 0.0))
                }
        # -------------------------------------------------------------------------
    else:
        remainders_by_type = {}
        final_remainder_details = {}

    # --- ИЗМЕНЕНИЕ: Фильтруем KPI по типу недвижимости, если он выбран ---
    volume_query = mysql_session.query(func.sum(EstateDeal.deal_sum)).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses)
    )
    if mysql_prop_key:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    total_deals_volume = volume_query.scalar() or 0

    income_query = mysql_session.query(func.sum(FinanceOperation.summa)).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        FinanceOperation.status_name == 'Проведено'
    )
    if mysql_prop_key:
        income_query = income_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    total_income = income_query.scalar() or 0
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    yearly_plan_fact = {
        'labels': [f"{i:02}" for i in range(1, 13)],
        'plan_volume': [0] * 12, 'fact_volume': [0] * 12,
        'plan_income': [0] * 12, 'fact_income': [0] * 12
    }

    # property_type тут 'Квартира' (русский)
    plans_query = planning_session.query(planning_models.SalesPlan).filter_by(complex_name=complex_name,
                                                                              year=today.year)
    if property_type:
        plans_query = plans_query.filter_by(property_type=property_type)
    for p in plans_query.all():
        yearly_plan_fact['plan_volume'][p.month - 1] += p.plan_volume
        yearly_plan_fact['plan_income'][p.month - 1] += p.plan_income

    fact_volume_by_month = [0] * 12
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    volume_query = mysql_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('total')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses),
        extract('year', effective_date) == today.year
    )
    if mysql_prop_key:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    for row in volume_query.group_by('month').all():
        fact_volume_by_month[row.month - 1] = row.total or 0
    yearly_plan_fact['fact_volume'] = fact_volume_by_month

    fact_income_by_month = [0] * 12
    income_query = mysql_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('total')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        FinanceOperation.status_name == 'Проведено',
        extract('year', FinanceOperation.date_added) == today.year
    )
    if mysql_prop_key:
        income_query = income_query.filter(EstateSell.estate_sell_category == mysql_prop_key)
    for row in income_query.group_by('month').all():
        fact_income_by_month[row.month - 1] = row.total or 0
    yearly_plan_fact['fact_income'] = fact_income_by_month

    deals_query = mysql_session.query(
        EstateDeal.id, EstateDeal.deal_sum, EstateSell.estate_sell_category.label('mysql_key'),
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).label('deal_date')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses)
    )
    if mysql_prop_key:
        deals_query = deals_query.filter(EstateSell.estate_sell_category == mysql_prop_key)

    recent_deals_raw = deals_query.order_by(
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).desc()
    ).limit(15).all()

    recent_deals = []
    for deal in recent_deals_raw:
        recent_deals.append({
            'id': deal.id,
            'deal_sum': deal.deal_sum,
            'property_type': map_mysql_key_to_russian_value(deal.mysql_key),
            'deal_date': deal.deal_date
        })

    remainders_chart_data = {"labels": [], "data": []}
    if remainders_by_type:
        remainders_chart_data["labels"] = list(remainders_by_type.keys())
        remainders_chart_data["data"] = [v['count'] for v in remainders_by_type.values()]

    sales_analysis = {"by_floor": {}, "by_rooms": {}, "by_area": {}}
    total_remainders_count = sum(v['count'] for v in remainders_by_type.values())
    type_to_analyze_russian = property_type if property_type else 'Квартира'
    type_to_analyze_mysql = map_russian_to_mysql_key(type_to_analyze_russian)

    if type_to_analyze_russian == 'Квартира':
        base_query = mysql_session.query(EstateSell).join(EstateDeal).join(EstateHouse).filter(
            EstateHouse.complex_name == complex_name,
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

    # --- НАЧАЛО: Логика для анализа стояков и остатков по этажам ---

    # 1. АНАЛИЗ ОСТАТКОВ ПО ЭТАЖАМ
    remainders_by_floor_query = mysql_session.query(
        EstateSell.estate_floor,
        func.count(EstateSell.id).label('unit_count')
    ).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateSell.estate_sell_status_name.in_(VALID_STATUSES)
    )
    if mysql_prop_key:
        remainders_by_floor_query = remainders_by_floor_query.filter(EstateSell.estate_sell_category == mysql_prop_key)

    remainders_by_floor_data = remainders_by_floor_query.group_by(EstateSell.estate_floor).order_by(
        EstateSell.estate_floor).all()

    remainders_by_floor_chart = {
        'labels': [f"{int(r.estate_floor)} этаж" for r in remainders_by_floor_data if r.estate_floor is not None],
        'data': [r.unit_count for r in remainders_by_floor_data if r.estate_floor is not None]
    }

    # 2. АНАЛИЗ СТОЯКОВ (ПРОДАНО)
    # --- ИСПРАВЛЕНИЕ: ДОБАВЛЕНЫ estate_sell_category В SELECT И GROUP BY ---
    riser_sold_query = mysql_session.query(
        EstateHouse.name.label('house_name'),
        EstateSell.estate_sell_category,  # <-- ИСПРАВЛЕНИЕ
        EstateSell.estate_rooms,
        EstateSell.estate_area,
        func.count(EstateDeal.id).label('sold_count')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses)
    )
    # (Фильтр по prop_type убран, чтобы JS мог фильтровать)

    riser_sold_data = riser_sold_query.group_by(
        EstateHouse.name,
        EstateSell.estate_sell_category,  # <-- ИСПРАВЛЕНИЕ
        EstateSell.estate_rooms,
        EstateSell.estate_area
    ).all()
    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    # 3. АНАЛИЗ СТОЯКОВ (ОСТАТКИ)
    riser_remain_query = mysql_session.query(
        EstateHouse.name.label('house_name'),
        EstateSell.estate_sell_category,  # <-- Нам нужен тип недвижимости
        EstateSell.estate_rooms,
        EstateSell.estate_area,
        func.count(EstateSell.id).label('remaining_count')
    ).join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        EstateHouse.complex_name == complex_name,
        EstateSell.estate_sell_status_name.in_(VALID_STATUSES)
    )
    # (Фильтр по prop_type убран, чтобы JS мог фильтровать)

    riser_remain_data = riser_remain_query.group_by(
        EstateHouse.name, EstateSell.estate_sell_category, EstateSell.estate_rooms, EstateSell.estate_area
    ).all()

    # 4. ОБЪЕДИНЕНИЕ ДАННЫХ ПО СТОЯКАМ (с добавлением prop_type)
    riser_analysis = {}
    all_houses = set()
    all_prop_types = set()

    # Обрабатываем ПРОДАННЫЕ
    for r in riser_sold_data:
        prop_type_ru = map_mysql_key_to_russian_value(r.estate_sell_category)
        key = (r.house_name, prop_type_ru, int(r.estate_rooms) if r.estate_rooms else 0,
               float(r.estate_area) if r.estate_area else 0.0)

        if key not in riser_analysis:
            riser_analysis[key] = {'sold': 0, 'remaining': 0}
        riser_analysis[key]['sold'] = r.sold_count

        all_houses.add(r.house_name)
        all_prop_types.add(prop_type_ru)

    # Обрабатываем ОСТАТКИ
    for r in riser_remain_data:
        prop_type_ru = map_mysql_key_to_russian_value(r.estate_sell_category)
        key = (r.house_name, prop_type_ru, int(r.estate_rooms) if r.estate_rooms else 0,
               float(r.estate_area) if r.estate_area else 0.0)

        if key not in riser_analysis:
            riser_analysis[key] = {'sold': 0, 'remaining': 0}
        riser_analysis[key]['remaining'] = r.remaining_count

        all_houses.add(r.house_name)
        all_prop_types.add(prop_type_ru)

    # 5. ФОРМАТИРОВАНИЕ ДЛЯ JS (плоский список)
    riser_data_for_js = [
        {
            'house': k[0],
            'prop_type': k[1],
            'rooms': k[2],
            'area': k[3],
            'sold': v['sold'],
            'remaining': v['remaining']
        }
        for k, v in riser_analysis.items()
    ]

    riser_filter_options = {
        'houses': sorted(list(all_houses)),
        'prop_types': sorted(list(all_prop_types))
    }
    # --- КОНЕЦ ЛОГИКИ СТОЯКОВ ---

    final_remainder_details = {}  # Изменено с defaultdict на чистый dict
    for prop_type, rooms_data in remainder_details.items():
        final_remainder_details[prop_type] = {}  # Инициализация внутреннего dict
        for rooms_key, data in rooms_data.items():
            final_remainder_details[prop_type][rooms_key] = {
                'count': int(data['count']),
                'total_price': float(data['total_price']),
                'total_area': float(data['total_area']),
                'avg_price_sqm': float(data.get('avg_price_sqm', 0.0))
            }
    dashboard_data = {
        "complex_name": complex_name,
        "kpi": {"total_deals_volume": total_deals_volume, "total_income": total_income,
                # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
                "remainders_by_type": remainders_by_type,
                "total_remainders_count": total_remainders_count  # <-- НОВОЕ ПОЛЕ
                },
        "charts": {
            "plan_fact_dynamics_yearly": yearly_plan_fact,
            "remainders_chart_data": remainders_chart_data,
            "remainder_details": final_remainder_details,
            "sales_analysis": sales_analysis,
            "price_dynamics": get_price_dynamics_data(complex_name, mysql_prop_key),

            # --- НОВЫЕ ДАННЫЕ ДЛЯ JS ---
            "remainders_by_floor": remainders_by_floor_chart,
            "riser_analysis_data": riser_data_for_js,
            "riser_filter_options": riser_filter_options
        },
        "recent_deals": recent_deals,
        "houses_data": houses_data,
    }
    return dashboard_data