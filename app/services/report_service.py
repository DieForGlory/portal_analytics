# app/services/report_service.py
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sqlalchemy import func, extract
from ..core.db_utils import get_planning_session, get_mysql_session
import io
from collections import defaultdict
from dateutil.relativedelta import relativedelta
# --- ИЗМЕНЕНИЯ ЗДЕСЬ: Обновляем импорты ---
from app.models import planning_models
from .data_service import get_all_complex_names
from ..models.estate_models import EstateDeal, EstateHouse, EstateSell
from ..models.finance_models import FinanceOperation
from . import currency_service
from sqlalchemy import func, or_
from app.models.estate_models import EstateSell, EstateDeal, EstateHouse
from app.models.finance_models import FinanceOperation
from app.models.planning_models import map_mysql_key_to_russian_value
from app.core.db_utils import get_mysql_session
# --- ДОБАВЛЕНЫ НУЖНЫЕ ИМПОРТЫ "ПЕРЕВОДЧИКОВ" ---
from app.models.planning_models import map_russian_to_mysql_key, map_mysql_key_to_russian_value, PropertyType, \
    PaymentMethod
from sqlalchemy.orm import joinedload


def get_deal_registry_report_data(page=1, per_page=50):
    mysql_session = get_mysql_session()

    query = mysql_session.query(EstateSell).options(
        joinedload(EstateSell.house),
        joinedload(EstateSell.deals),
        joinedload(EstateSell.finance_operations)
    ).join(EstateHouse)

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)

    report_data = []

    for sell in pagination.items:
        # Логика выбора валидной сделки (как исправляли ранее)
        valid_deals = [
            d for d in sell.deals
            if d.deal_status_name not in ["Не определён", "Отменено", None]
               and d.deal_program_name is not None
        ]
        deal = max(valid_deals, key=lambda d: d.id) if valid_deals else (
            max(sell.deals, key=lambda d: d.id) if sell.deals else None)

        # Расчет дат договора
        raw_contract_date = None
        if deal:
            raw_contract_date = deal.agreement_date or deal.preliminary_date

        # Техническое поле: дата договора на начало месяца
        contract_start_month = "-"
        if raw_contract_date:
            contract_start_month = raw_contract_date.replace(day=1).strftime('%d.%m.%Y')

        # Сбор финансов
        all_non_booking_dates = [
            op.date_added for op in sell.finance_operations
            if op.payment_type != "Бронь" and op.date_added is not None
        ]
        first_payment_date = min(all_non_booking_dates) if all_non_booking_dates else None

        report_data.append({
            'object_id': sell.id,
            'project_name': sell.house.complex_name if sell.house else "-",  # Название проекта
            'house_name': sell.house.name if sell.house else "-",  # Номер/имя дома
            'type': map_mysql_key_to_russian_value(sell.estate_sell_category),
            'payment_method': deal.deal_program_name if deal else "-",
            'contract_status': "Оплачен" if deal and any(
                op.status_name == "Проведено" and op.payment_type != "Бронь" for op in
                sell.finance_operations) else "Не оплачен",
            'first_payment_date': first_payment_date.strftime('%d.%m.%Y') if first_payment_date else "-",
            'agreement_date': raw_contract_date.strftime('%d.%m.%Y') if raw_contract_date else "-",
            'contract_start_month': contract_start_month,  # Техническое поле
            'agreement_number': (deal.agreement_number or deal.arles_agreement_num) if deal else "-",
            'deal_sum': deal.deal_sum if deal else 0,
            'area': sell.estate_area,
            'object_status': sell.estate_sell_status_name
        })

    return report_data, pagination


def generate_deal_registry_excel():
    mysql_session = get_mysql_session()
    all_sells = mysql_session.query(EstateSell).options(
        joinedload(EstateSell.house),
        joinedload(EstateSell.deals),
        joinedload(EstateSell.finance_operations)
    ).all()

    excel_data = []
    for sell in all_sells:
        valid_deals = [d for d in sell.deals if d.deal_status_name not in ["Не определён", "Отменено", None]]
        deal = max(valid_deals, key=lambda d: d.id) if valid_deals else None

        raw_date = (deal.agreement_date or deal.preliminary_date) if deal else None

        excel_data.append({
            'ID объекта': sell.id,
            'Проект': sell.house.complex_name if sell.house else "-",
            'Дом': sell.house.name if sell.house else "-",
            'Тип объекта': map_mysql_key_to_russian_value(sell.estate_sell_category),
            'Вид оплаты': deal.deal_program_name if deal else "-",
            'Статус договора': "Оплачен" if deal and any(
                op.status_name == "Проведено" and op.payment_type != "Бронь" for op in
                sell.finance_operations) else "Не оплачен",
            'Дата первой оплаты': min([op.date_added for op in sell.finance_operations if
                                       op.payment_type != "Бронь" and op.date_added]).strftime('%d.%m.%Y') if any(
                op.payment_type != "Бронь" and op.date_added for op in sell.finance_operations) else "-",
            'Дата договора': raw_date.strftime('%d.%m.%Y') if raw_date else "-",
            'Дата договора (нач. месяца)': raw_date.replace(day=1).strftime('%d.%m.%Y') if raw_date else "-",
            'Номер договора': (deal.agreement_number or deal.arles_agreement_num) if deal else "-",
            'Стоимость по договору': deal.deal_sum if deal else 0,
            'Площадь': sell.estate_area,
            'Статус объекта': sell.estate_sell_status_name
        })

    df = pd.DataFrame(excel_data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Реестр сделок')
    output.seek(0)
    return output


def generate_plan_fact_excel_from_data(report_data, totals, year, month, period, property_type):
    if not report_data:
        return None

    df = pd.DataFrame(report_data)

    # Обработка вложенного словаря expected_income
    df['expected_income'] = df['expected_income'].apply(lambda x: x.get('sum', 0) if isinstance(x, dict) else x)

    # Подготовка строки итогов
    totals_row = totals.copy()
    if isinstance(totals_row.get('expected_income'), dict):
        totals_row['expected_income'] = totals_row['expected_income'].get('sum', 0)

    totals_df = pd.DataFrame([totals_row])
    totals_df.insert(0, 'complex_name', f'Итого ({property_type})')

    # Определение колонок
    ordered_columns = [
        'complex_name', 'plan_units', 'fact_units', 'percent_fact_units',
        'plan_volume', 'fact_volume', 'percent_fact_volume',
        'plan_income', 'fact_income', 'percent_fact_income', 'expected_income'
    ]

    # Фильтруем колонки, которые могут отсутствовать в периодах (например, прогнозы)
    avail_cols = [c for c in ordered_columns if c in df.columns]

    final_df = pd.concat([df[avail_cols], totals_df[avail_cols]], ignore_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        sheet_name = f"{period}_{year}"
        final_df.to_excel(writer, index=False, sheet_name=sheet_name[:31])

    output.seek(0)
    return output

def generate_zero_mortgage_template_excel():
    """
    Генерирует Excel-шаблон для матрицы "Ипотеки под 0%" в формате как на изображении.
    """
    # (Код этой функции без изменений)
    data = {
        'Месяц': [12, 18, 24, 30, 36, 42, 48, 54, 60],
        '30': [11, 16, 21, 26, 31, 36, 41, 45, 50],
        '40': [10, 14, 18, 22, 26, 31, 35, 39, 43],
        '50': [8, 12, 15, 19, 22, 26, 29, 33, 36],
        '60': [7, 9, 12, 15, 18, 21, 23, 26, 29]
    }
    df = pd.DataFrame(data)
    df.rename(columns={'30': 0.3, '40': 0.4, '50': 0.5, '60': 0.6}, inplace=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Zero Mortgage Matrix', startrow=1)
        workbook = writer.book
        worksheet = writer.sheets['Zero Mortgage Matrix']

        header_format = workbook.add_format({
            'bold': True, 'align': 'center', 'valign': 'vcenter',
            'fg_color': '#C6E0B4', 'border': 1
        })
        percent_format = workbook.add_format({'num_format': '0%', 'align': 'center', 'border': 1})
        month_format = workbook.add_format({'align': 'center', 'border': 1, 'bold': True, 'fg_color': '#C6E0B4'})

        worksheet.merge_range('B1:E1', 'ПВ', header_format)
        worksheet.write('A2', 'Месяц', header_format)
        for col_num, value in enumerate(df.columns.values):
            if col_num > 0:
                worksheet.write(1, col_num, value, header_format)

        worksheet.set_column('A:A', 10, month_format)
        worksheet.set_column('B:E', 10, percent_format)

    output.seek(0)
    return output


def generate_consolidated_report_by_period(year: int, period: str, property_type: str):
    """
    Генерирует сводный отчет за период, корректно агрегируя словари ожидаемых поступлений.
    """
    PERIOD_MONTHS = {
        'q1': range(1, 4), 'q2': range(4, 7), 'q3': range(7, 10), 'q4': range(10, 13),
        'h1': range(1, 7), 'h2': range(7, 13),
    }

    months_in_period = PERIOD_MONTHS.get(period)
    if not months_in_period:
        return [], {}

    # Используем структуру для хранения агрегированных данных
    aggregated_data = defaultdict(lambda: {
        'plan_units': 0, 'fact_units': 0, 'plan_volume': 0, 'fact_volume': 0,
        'plan_income': 0, 'fact_income': 0,
        'expected_income': {'sum': 0, 'ids': []}
    })
    aggregated_totals = defaultdict(float)
    aggregated_totals['expected_income_ids'] = []

    for month in months_in_period:
        monthly_data, monthly_totals, _ = generate_plan_fact_report(year, month, property_type)

        for project_row in monthly_data:
            complex_name = project_row['complex_name']
            aggregated_data[complex_name]['complex_name'] = complex_name
            for key in ['plan_units', 'fact_units', 'plan_volume', 'fact_volume', 'plan_income', 'fact_income']:
                aggregated_data[complex_name][key] += project_row.get(key, 0)

            # Агрегация словаря ожидаемых поступлений
            e_inc = project_row.get('expected_income', {'sum': 0, 'ids': []})
            aggregated_data[complex_name]['expected_income']['sum'] += e_inc.get('sum', 0)
            aggregated_data[complex_name]['expected_income']['ids'].extend(e_inc.get('ids', []))

        # Агрегация итогов
        for key in ['plan_units', 'fact_units', 'plan_volume', 'fact_volume', 'plan_income', 'fact_income']:
            aggregated_totals[key] += monthly_totals.get(key, 0)
        aggregated_totals['expected_income'] += monthly_totals.get('expected_income', 0)
        aggregated_totals['expected_income_ids'].extend(monthly_totals.get('expected_income_ids', []))

    final_report_data = []
    for complex_name, data in aggregated_data.items():
        data['percent_fact_units'] = (data['fact_units'] / data['plan_units'] * 100) if data['plan_units'] > 0 else 0
        data['percent_fact_volume'] = (data['fact_volume'] / data['plan_volume'] * 100) if data[
                                                                                               'plan_volume'] > 0 else 0
        data['percent_fact_income'] = (data['fact_income'] / data['plan_income'] * 100) if data[
                                                                                               'plan_income'] > 0 else 0
        data['forecast_units'] = 0
        data['forecast_volume'] = 0
        final_report_data.append(data)

    for k in ['units', 'volume', 'income']:
        aggregated_totals[f'percent_fact_{k}'] = (
                    aggregated_totals[f'fact_{k}'] / aggregated_totals[f'plan_{k}'] * 100) if aggregated_totals[
                                                                                                  f'plan_{k}'] > 0 else 0

    final_report_data.sort(key=lambda x: x['complex_name'])
    return final_report_data, dict(aggregated_totals)


def get_fact_income_data(year: int, month: int, property_type: str):
    """Собирает ФАКТИЧЕСКИЕ поступления (статус 'Проведено')."""
    mysql_session = get_mysql_session()
    query = mysql_session.query(
        EstateHouse.complex_name, func.sum(FinanceOperation.summa).label('fact_income')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        FinanceOperation.status_name == "Проведено",
        extract('year', FinanceOperation.date_added) == year,
        extract('month', FinanceOperation.date_added) == month,
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки",
        FinanceOperation.payment_type != "Уступка права требования",
    )
    if property_type != 'All':
        # --- ИСПРАВЛЕНИЕ: Переводим 'Квартира' в 'flat' ---
        mysql_key = map_russian_to_mysql_key(property_type)
        query = query.filter(EstateSell.estate_sell_category == mysql_key)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    results = query.group_by(EstateHouse.complex_name).all()
    return {row.complex_name: (row.fact_income or 0) for row in results}


def get_expected_income_data(year: int, month: int, property_type: str):
    """
    Собирает ОЖИДАЕМЫЕ поступления (ИСКЛЮЧАЯ возвраты), их сумму и ID операций.
    """
    mysql_session = get_mysql_session()
    query = mysql_session.query(
        EstateHouse.complex_name,
        func.sum(FinanceOperation.summa).label('expected_income'),
        func.group_concat(FinanceOperation.id).label('income_ids')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        FinanceOperation.status_name == "К оплате",
        extract('year', FinanceOperation.date_to) == year,
        extract('month', FinanceOperation.date_to) == month,
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки"
    )
    if property_type != 'All':
        # --- ИСПРАВЛЕНИЕ: Переводим 'Квартира' в 'flat' ---
        mysql_key = map_russian_to_mysql_key(property_type)
        query = query.filter(EstateSell.estate_sell_category == mysql_key)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    results = query.group_by(EstateHouse.complex_name).all()
    data = {}
    for row in results:
        ids = [int(id_str) for id_str in row.income_ids.split(',')] if row.income_ids else []
        data[row.complex_name] = {'sum': row.expected_income or 0, 'ids': ids}
    return data


def get_refund_data(year: int, month: int, property_type: str):
    """
    Собирает данные по ВОЗВРАТАМ, запланированным на указанный период.
    """
    mysql_session = get_mysql_session()
    query = mysql_session.query(
        func.sum(FinanceOperation.summa).label('total_refunds')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .filter(
        FinanceOperation.status_name == "К оплате",
        extract('year', FinanceOperation.date_to) == year,
        extract('month', FinanceOperation.date_to) == month,
        FinanceOperation.payment_type == "Возврат поступлений при отмене сделки"
    )
    if property_type != 'All':
        # --- ИСПРАВЛЕНИЕ: Переводим 'Квартира' в 'flat' ---
        mysql_key = map_russian_to_mysql_key(property_type)
        query = query.filter(EstateSell.estate_sell_category == mysql_key)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    return query.scalar() or 0.0


def get_plan_income_data(year: int, month: int, property_type: str):
    """Получает плановые данные по поступлениям."""
    planning_session = get_planning_session()
    query = planning_session.query(planning_models.SalesPlan).filter_by(year=year, month=month)
    if property_type != 'All':
        query = query.filter_by(property_type=property_type)  # Здесь 'Квартира' - это правильно

    results = query.all()
    plan_data = defaultdict(float)
    for row in results:
        plan_data[row.complex_name] += row.plan_income
    return plan_data


def generate_ids_excel(ids_str: str):
    """
    Создает Excel-файл из списка ID.
    """
    # (Код этой функции без изменений)
    try:
        ids = [int(id_val) for id_val in ids_str.split(',')]
    except (ValueError, AttributeError):
        return None

    df = pd.DataFrame(ids, columns=['ID Финансовой операции'])
    output = io.BytesIO()
    df.to_excel(output, index=False, sheet_name='IDs')
    output.seek(0)
    return output


def get_fact_data(year: int, month: int, property_type: str):
    """Собирает фактические данные о продажах из БД."""
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    mysql_session = get_mysql_session()
    query = mysql_session.query(
        EstateHouse.complex_name,
        func.count(EstateDeal.id).label('fact_units')
    ).join(
        EstateSell, EstateDeal.estate_sell_id == EstateSell.id
    ).join(
        EstateHouse, EstateSell.house_id == EstateHouse.id
    ).filter(
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"]),
        extract('year', effective_date) == year,
        extract('month', effective_date) == month,
    )
    if property_type != 'All':
        # --- ИСПРАВЛЕНИЕ: Переводим 'Квартира' в 'flat' ---
        mysql_key = map_russian_to_mysql_key(property_type)
        query = query.filter(EstateSell.estate_sell_category == mysql_key)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    results = query.group_by(EstateHouse.complex_name).all()
    return {row.complex_name: row.fact_units for row in results}


def get_plan_data(year: int, month: int, property_type: str):
    """Получает плановые данные из нашей таблицы SalesPlan."""
    planning_session = get_planning_session()
    query = planning_session.query(planning_models.SalesPlan).filter_by(year=year, month=month)
    if property_type != 'All':
        query = query.filter_by(property_type=property_type)  # Здесь 'Квартира' - это правильно

    results = query.all()
    plan_data = defaultdict(int)
    for row in results:
        plan_data[row.complex_name] += row.plan_units
    return plan_data


def generate_plan_fact_report(year: int, month: int, property_type: str):
    """Основная функция для генерации отчета, возвращающая детализацию, итоги и возвраты."""
    # (Код этой функции без изменений, т.к. исправления внесены в вызываемые ею функции)
    plan_units_data = get_plan_data(year, month, property_type)
    fact_units_data = get_fact_data(year, month, property_type)
    plan_volume_data = get_plan_volume_data(year, month, property_type)
    fact_volume_data = get_fact_volume_data(year, month, property_type)
    plan_income_data = get_plan_income_data(year, month, property_type)
    fact_income_data = get_fact_income_data(year, month, property_type)
    all_expected_income_data = get_expected_income_data(year, month, property_type)

    total_refunds = get_refund_data(year, month, property_type)

    all_complexes = sorted(
        list(set(plan_units_data.keys()) | set(fact_units_data.keys()) | set(plan_income_data.keys())))

    report_data = []
    totals = {
        'plan_units': 0, 'fact_units': 0, 'plan_volume': 0, 'fact_volume': 0,
        'plan_income': 0, 'fact_income': 0, 'expected_income': 0
    }

    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01',
                                      today) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    for complex_name in all_complexes:
        plan_units = plan_units_data.get(complex_name, 0)
        fact_units = fact_units_data.get(complex_name, 0)
        plan_volume = plan_volume_data.get(complex_name, 0)
        fact_volume = fact_volume_data.get(complex_name, 0)
        plan_income = plan_income_data.get(complex_name, 0)
        fact_income = fact_income_data.get(complex_name, 0)
        complex_expected_income = all_expected_income_data.get(complex_name, {'sum': 0, 'ids': []})

        percent_fact_units = (fact_units / plan_units) * 100 if plan_units > 0 else 0
        forecast_units = ((
                                  fact_units / passed_workdays) * workdays_in_month / plan_units) * 100 if plan_units > 0 else 0
        percent_fact_volume = (fact_volume / plan_volume) * 100 if plan_volume > 0 else 0
        forecast_volume = ((
                                   fact_volume / passed_workdays) * workdays_in_month / plan_volume) * 100 if plan_volume > 0 else 0
        percent_fact_income = (fact_income / plan_income) * 100 if plan_income > 0 else 0

        totals['plan_units'] += plan_units
        totals['fact_units'] += fact_units
        totals['plan_volume'] += plan_volume
        totals['fact_volume'] += fact_volume
        totals['plan_income'] += plan_income
        totals['fact_income'] += fact_income
        totals['expected_income'] += complex_expected_income['sum']
        totals.setdefault('expected_income_ids', []).extend(complex_expected_income['ids'])

        report_data.append({
            'complex_name': complex_name,
            'plan_units': plan_units, 'fact_units': fact_units, 'percent_fact_units': percent_fact_units,
            'forecast_units': forecast_units,
            'plan_volume': plan_volume, 'fact_volume': fact_volume, 'percent_fact_volume': percent_fact_volume,
            'forecast_volume': forecast_volume,
            'plan_income': plan_income, 'fact_income': fact_income, 'percent_fact_income': percent_fact_income,
            'expected_income': complex_expected_income
        })

    totals['percent_fact_units'] = (totals['fact_units'] / totals['plan_units']) * 100 if totals[
                                                                                              'plan_units'] > 0 else 0
    totals['forecast_units'] = ((totals['fact_units'] / passed_workdays) * workdays_in_month / totals[
        'plan_units']) * 100 if totals['plan_units'] > 0 else 0
    totals['percent_fact_volume'] = (totals['fact_volume'] / totals['plan_volume']) * 100 if totals[
                                                                                                 'plan_volume'] > 0 else 0
    totals['forecast_volume'] = ((totals['fact_volume'] / passed_workdays) * workdays_in_month / totals[
        'plan_volume']) * 100 if totals['plan_volume'] > 0 else 0
    totals['percent_fact_income'] = (totals['fact_income'] / totals['plan_income']) * 100 if totals[
                                                                                                 'plan_income'] > 0 else 0

    return report_data, totals, total_refunds


def process_plan_from_excel(file_path: str, year: int, month: int):
    # (Код этой функции без изменений)
    df = pd.read_excel(file_path)
    planning_session = get_planning_session()
    for index, row in df.iterrows():
        plan_entry = planning_session.query(planning_models.SalesPlan).filter_by(
            year=year, month=month, complex_name=row['ЖК'], property_type=row['Тип недвижимости']
        ).first()
        if not plan_entry:
            plan_entry = planning_models.SalesPlan(year=year, month=month, complex_name=row['ЖК'],
                                                   property_type=row['Тип недвижимости'])
            planning_session.add(plan_entry)
        plan_entry.plan_units = row['План, шт']
        plan_entry.plan_volume = row['План контрактации, UZS']
        plan_entry.plan_income = row['План поступлений, UZS']
    planning_session.commit()
    return f"Успешно обработано {len(df)} строк."


def generate_plan_template_excel():
    # (Код этой функции без изменений)
    complex_names = get_all_complex_names()
    property_types = list(planning_models.PropertyType)
    headers = ['ЖК', 'Тип недвижимости', 'План, шт', 'План контрактации, UZS', 'План поступлений, UZS']
    data = [{'ЖК': name, 'Тип недвижимости': prop_type.value, 'План, шт': 0, 'План контрактации, UZS': 0,
             'План поступлений, UZS': 0} for name in complex_names for prop_type in property_types]
    df = pd.DataFrame(data, columns=headers)
    output = io.BytesIO()
    df.to_excel(output, index=False, sheet_name='Шаблон плана')
    output.seek(0)
    return output


def get_monthly_summary_by_property_type(year: int, month: int):
    """
    Собирает сводку по каждому типу недвижимости, включая ID для ссылок.
    """
    # (Код этой функции без изменений, т.к. она вызывает исправленные нами функции)
    summary_data = []
    property_types = list(planning_models.PropertyType)
    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01', today.strftime(
        '%Y-%m-%d')) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    for prop_type in property_types:
        total_plan_units = sum(get_plan_data(year, month, prop_type.value).values())
        total_fact_units = sum(get_fact_data(year, month, prop_type.value).values())
        total_plan_volume = sum(get_plan_volume_data(year, month, prop_type.value).values())
        total_fact_volume = sum(get_fact_volume_data(year, month, prop_type.value).values())
        total_plan_income = sum(get_plan_income_data(year, month, prop_type.value).values())
        total_fact_income = sum(get_fact_income_data(year, month, prop_type.value).values())

        expected_income_data = get_expected_income_data(year, month, prop_type.value)
        total_expected_income_sum = sum(v['sum'] for v in expected_income_data.values())
        total_expected_income_ids = [id_val for v in expected_income_data.values() for id_val in v['ids']]

        if (
                total_plan_units + total_fact_units + total_plan_volume + total_fact_volume + total_plan_income + total_fact_income) == 0:
            continue

        percent_fact_units = (total_fact_units / total_plan_units) * 100 if total_plan_units > 0 else 0
        forecast_units = ((
                                  total_fact_units / passed_workdays) * workdays_in_month / total_plan_units) * 100 if total_plan_units > 0 else 0
        percent_fact_volume = (total_fact_volume / total_plan_volume) * 100 if total_plan_volume > 0 else 0
        forecast_volume = ((
                                   total_fact_volume / passed_workdays) * workdays_in_month / total_plan_volume) * 100 if total_plan_volume > 0 else 0
        percent_fact_income = (total_fact_income / total_plan_income) * 100 if total_plan_income > 0 else 0

        summary_data.append({
            'property_type': prop_type.value,
            'total_plan_units': total_plan_units,
            'total_fact_units': total_fact_units,
            'percent_fact_units': percent_fact_units,
            'forecast_units': forecast_units,
            'total_plan_volume': total_plan_volume,
            'total_fact_volume': total_fact_volume,
            'percent_fact_volume': percent_fact_volume,
            'forecast_volume': forecast_volume,
            'total_plan_income': total_plan_income,
            'total_fact_income': total_fact_income,
            'percent_fact_income': percent_fact_income,
            'total_expected_income': {
                'sum': total_expected_income_sum,
                'ids': total_expected_income_ids
            }
        })
    return summary_data


def get_fact_volume_data(year: int, month: int, property_type: str):
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    mysql_session = get_mysql_session()
    query = mysql_session.query(
        EstateHouse.complex_name, func.sum(EstateDeal.deal_sum).label('fact_volume')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id).join(EstateHouse,
                                                                        EstateSell.house_id == EstateHouse.id).filter(
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"]),
        extract('year', effective_date) == year,
        extract('month', effective_date) == month
    )
    if property_type != 'All':
        # --- ИСПРАВЛЕНИЕ: Переводим 'Квартира' в 'flat' ---
        mysql_key = map_russian_to_mysql_key(property_type)
        query = query.filter(EstateSell.estate_sell_category == mysql_key)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    results = query.group_by(EstateHouse.complex_name).all()
    return {row.complex_name: (row.fact_volume or 0) for row in results}


def get_plan_volume_data(year: int, month: int, property_type: str):
    """Получает плановые данные по объему контрактации."""
    planning_session = get_planning_session()
    query = planning_session.query(planning_models.SalesPlan).filter_by(year=year, month=month)
    if property_type != 'All':
        query = query.filter_by(property_type=property_type)  # Здесь 'Квартира' - это правильно

    results = query.all()
    plan_data = defaultdict(float)
    for row in results:
        plan_data[row.complex_name] += row.plan_volume
    return plan_data


def generate_plan_fact_excel(year: int, month: int, property_type: str, period: str = 'monthly', currency: str = 'UZS',
                             rate: float = 1.0):
    """
    Универсальная генерация Excel с поддержкой периодов и конвертации валюты.
    """
    if period != 'monthly':
        report_data, totals = generate_consolidated_report_by_period(year, period, property_type)
    else:
        report_data, totals, _ = generate_plan_fact_report(year, month, property_type)

    if not report_data:
        return None

    # Подготовка данных: извлекаем суммы из словарей
    for row in report_data:
        if isinstance(row.get('expected_income'), dict):
            row['expected_income'] = row['expected_income'].get('sum', 0)

    if isinstance(totals.get('expected_income'), dict):
        totals['expected_income'] = totals['expected_income'].get('sum', 0)

    df = pd.DataFrame(report_data)
    totals_df = pd.DataFrame([totals])

    # Применение курса валют
    if currency == 'USD' and rate > 1:
        money_cols = ['plan_volume', 'fact_volume', 'plan_income', 'fact_income', 'expected_income']
        for col in money_cols:
            if col in df.columns:
                df[col] = df[col] / rate
            if col in totals_df.columns:
                totals_df[col] = totals_df[col] / rate

    # Формирование финальной таблицы
    ordered_columns = [
        'complex_name', 'plan_units', 'fact_units', 'percent_fact_units',
        'plan_volume', 'fact_volume', 'percent_fact_volume',
        'plan_income', 'fact_income', 'percent_fact_income', 'expected_income'
    ]

    # Добавляем колонки прогнозов только для месячного отчета
    if period == 'monthly':
        ordered_columns.insert(4, 'forecast_units')
        ordered_columns.insert(8, 'forecast_volume')

    renamed_columns = [
        'Проект', 'План, шт', 'Факт, шт', '% Вып. (шт)',
        f'План контр., {currency}', f'Факт контр., {currency}', '% Вып. (контр.)',
        f'План пост., {currency}', f'Факт пост., {currency}', '% Вып. (пост.)', f'Ожид. пост., {currency}'
    ]
    if period == 'monthly':
        renamed_columns.insert(4, '% Прогноз (шт)')
        renamed_columns.insert(8, '% Прогноз (контр.)')

    df = df[ordered_columns]
    totals_df = totals_df[[c for c in ordered_columns if c != 'complex_name']]
    totals_df.insert(0, 'complex_name', f'Итого ({property_type})')

    final_df = pd.concat([df, totals_df], ignore_index=True)
    final_df.columns = renamed_columns

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name=f'Report {period}')
    output.seek(0)
    return output


def get_sales_pace_comparison_data():
    """
    Собирает данные по темпам продаж (кол-во сделок) для ВСЕХ проектов
    начиная с 2022 года.
    Возвращает структуру для Chart.js, включая агрегированные данные.
    """
    mysql_session = get_mysql_session()
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    start_date = date(2022, 1, 1)
    today = date.today()

    # 1. Запрашиваем данные по всем ЖК
    query = mysql_session.query(
        EstateHouse.complex_name,
        extract('year', effective_date).label('year'),
        extract('month', effective_date).label('month'),
        func.count(EstateDeal.id).label('cnt')
    ).select_from(EstateDeal).join(EstateSell).join(EstateHouse).filter(
        EstateDeal.deal_status_name.in_(["Сделка в работе", "Сделка проведена"]),
        effective_date >= start_date
    ).group_by(
        EstateHouse.complex_name, 'year', 'month'
    ).all()

    # 2. Организуем данные: complex -> "YYYY-MM" -> count
    data_map = defaultdict(lambda: defaultdict(int))
    all_complexes = set()

    for row in query:
        key = f"{int(row.year)}-{int(row.month):02d}"
        data_map[row.complex_name][key] = row.cnt
        all_complexes.add(row.complex_name)

    # 3. Формируем помесячные данные (Monthly)
    timeline_months = []
    timeline_labels_monthly = []
    curr = start_date
    while curr <= today:
        key = f"{curr.year}-{curr.month:02d}"
        timeline_months.append(key)
        timeline_labels_monthly.append(f"{curr.month:02d}.{curr.year}")
        curr += relativedelta(months=1)

    datasets_monthly = []
    aggregate_monthly = [0] * len(timeline_months)  # Массив для суммы

    sorted_complexes = sorted(list(all_complexes))

    for complex_name in sorted_complexes:
        data_points = [data_map[complex_name][m] for m in timeline_months]

        # Складываем в общий котел
        for i, val in enumerate(data_points):
            aggregate_monthly[i] += val

        datasets_monthly.append({
            'label': complex_name,
            'data': data_points,
            'fill': False
        })

    # 4. Формируем погодовые данные (Yearly)
    years = sorted(list(set([m[:4] for m in timeline_months])))
    datasets_yearly = []
    aggregate_yearly = [0] * len(years)

    for complex_name in sorted_complexes:
        yearly_counts = defaultdict(int)
        for m_key, val in data_map[complex_name].items():
            y_key = m_key[:4]
            yearly_counts[y_key] += val

        data_points = [yearly_counts[y] for y in years]

        # Складываем в общий котел
        for i, val in enumerate(data_points):
            aggregate_yearly[i] += val

        datasets_yearly.append({
            'label': complex_name,
            'data': data_points
        })

    return {
        'complex_list': sorted_complexes,  # Список имен проектов для выпадающего списка
        'monthly': {
            'labels': timeline_labels_monthly,
            'datasets': datasets_monthly,
            'aggregate': aggregate_monthly
        },
        'yearly': {
            'labels': years,
            'datasets': datasets_yearly,
            'aggregate': aggregate_yearly
        }
    }


def generate_annual_report_excel(year: int):
    """
    Генерирует годовой отчет в Excel с тремя листами:
    1. Свод по месяцам (Итого)
    2. Детализация по проектам (за каждый месяц)
    3. Детализация по типам недвижимости (за каждый месяц)
    """
    all_projects_rows = []
    all_types_rows = []
    all_months_rows = []

    # Проходим по всем месяцам года
    for month in range(1, 13):
        # 1. Данные по проектам (используем 'All' для всех типов недвижимости внутри проекта)
        proj_data, totals, _ = generate_plan_fact_report(year, month, 'All')

        # Обработка данных по проектам
        for row in proj_data:
            r = row.copy()
            r['month'] = month
            # Извлекаем сумму из словаря expected_income, если это словарь
            if isinstance(r.get('expected_income'), dict):
                r['expected_income'] = r['expected_income'].get('sum', 0)
            all_projects_rows.append(r)

        # 2. Данные по типам недвижимости
        type_data = get_monthly_summary_by_property_type(year, month)
        for row in type_data:
            r = row.copy()
            r['month'] = month
            # Извлекаем сумму из total_expected_income
            if isinstance(r.get('total_expected_income'), dict):
                r['expected_income'] = r['total_expected_income'].get('sum', 0)
            else:
                r['expected_income'] = 0
            # Удаляем сложный объект, чтобы не мешал созданию DF
            if 'total_expected_income' in r:
                del r['total_expected_income']
            all_types_rows.append(r)

        # 3. Общие итоги за месяц
        t = totals.copy()
        t['month'] = month
        # totals['expected_income'] обычно float, но на всякий случай проверим
        if isinstance(t.get('expected_income'), dict):
            t['expected_income'] = t['expected_income'].get('sum', 0)
        # Удаляем список ID, он не нужен в Excel
        if 'expected_income_ids' in t:
            del t['expected_income_ids']
        all_months_rows.append(t)

    # --- Формируем DataFrame ---

    # Лист 1: Свод по месяцам
    df_months = pd.DataFrame(all_months_rows)
    month_cols_map = {
        'month': 'Месяц',
        'plan_units': 'План (шт)', 'fact_units': 'Факт (шт)', 'percent_fact_units': '% Вып. (шт)',
        'plan_volume': 'План (контрактация)', 'fact_volume': 'Факт (контрактация)',
        'percent_fact_volume': '% Вып. (контр.)',
        'plan_income': 'План (поступления)', 'fact_income': 'Факт (поступления)',
        'percent_fact_income': '% Вып. (пост.)',
        'expected_income': 'Ожидаемые поступления'
    }
    # Оставляем только существующие колонки
    avail_cols = [c for c in month_cols_map.keys() if c in df_months.columns]
    df_months = df_months[avail_cols].rename(columns=month_cols_map)

    # Лист 2: По проектам
    df_projects = pd.DataFrame(all_projects_rows)
    proj_cols_map = {
        'month': 'Месяц',
        'complex_name': 'Проект',
        'plan_units': 'План (шт)', 'fact_units': 'Факт (шт)', 'percent_fact_units': '% Вып. (шт)',
        'plan_volume': 'План (контрактация)', 'fact_volume': 'Факт (контрактация)',
        'percent_fact_volume': '% Вып. (контр.)',
        'plan_income': 'План (поступления)', 'fact_income': 'Факт (поступления)',
        'percent_fact_income': '% Вып. (пост.)',
        'expected_income': 'Ожидаемые поступления'
    }
    avail_cols = [c for c in proj_cols_map.keys() if c in df_projects.columns]
    # Сортируем: сначала месяц, потом имя проекта
    if 'complex_name' in df_projects.columns:
        df_projects.sort_values(by=['month', 'complex_name'], inplace=True)
    df_projects = df_projects[avail_cols].rename(columns=proj_cols_map)

    # Лист 3: По типам недвижимости
    df_types = pd.DataFrame(all_types_rows)
    type_cols_map = {
        'month': 'Месяц',
        'property_type': 'Тип недвижимости',
        'total_plan_units': 'План (шт)', 'total_fact_units': 'Факт (шт)', 'percent_fact_units': '% Вып. (шт)',
        'total_plan_volume': 'План (контрактация)', 'total_fact_volume': 'Факт (контрактация)',
        'percent_fact_volume': '% Вып. (контр.)',
        'total_plan_income': 'План (поступления)', 'total_fact_income': 'Факт (поступления)',
        'percent_fact_income': '% Вып. (пост.)',
        'expected_income': 'Ожидаемые поступления'
    }
    avail_cols = [c for c in type_cols_map.keys() if c in df_types.columns]
    if 'property_type' in df_types.columns:
        df_types.sort_values(by=['month', 'property_type'], inplace=True)
    df_types = df_types[avail_cols].rename(columns=type_cols_map)

    # Запись в Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_months.to_excel(writer, sheet_name='Свод по месяцам', index=False)
        df_projects.to_excel(writer, sheet_name='По проектам', index=False)
        df_types.to_excel(writer, sheet_name='По типам', index=False)

    output.seek(0)
    return output

def calculate_grand_totals(year, month):
    """
    Рассчитывает общие итоговые показатели, включая ID для ссылок.
    """
    # (Код этой функции без изменений, т.к. она вызывает исправленные нами функции)
    summary_by_type = get_monthly_summary_by_property_type(year, month)

    if not summary_by_type:
        return {}

    grand_totals = {
        'plan_units': sum(item.get('total_plan_units', 0) for item in summary_by_type),
        'fact_units': sum(item.get('total_fact_units', 0) for item in summary_by_type),
        'plan_volume': sum(item.get('total_plan_volume', 0) for item in summary_by_type),
        'fact_volume': sum(item.get('total_fact_volume', 0) for item in summary_by_type),
        'plan_income': sum(item.get('total_plan_income', 0) for item in summary_by_type),
        'fact_income': sum(item.get('total_fact_income', 0) for item in summary_by_type),
        'expected_income': sum(item['total_expected_income']['sum'] for item in summary_by_type),
        'expected_income_ids': [id_val for item in summary_by_type for id_val in item['total_expected_income']['ids']]
    }

    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01',
                                      today) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    grand_totals['percent_fact_units'] = (grand_totals['fact_units'] / grand_totals['plan_units'] * 100) if \
        grand_totals['plan_units'] > 0 else 0
    grand_totals['forecast_units'] = ((grand_totals['fact_units'] / passed_workdays) * workdays_in_month / grand_totals[
        'plan_units'] * 100) if grand_totals['plan_units'] > 0 else 0

    grand_totals['percent_fact_volume'] = (grand_totals['fact_volume'] / grand_totals['plan_volume'] * 100) if \
        grand_totals['plan_volume'] > 0 else 0
    grand_totals['forecast_volume'] = (
            (grand_totals['fact_volume'] / passed_workdays) * workdays_in_month / grand_totals[
        'plan_volume'] * 100) if grand_totals['plan_volume'] > 0 else 0

    grand_totals['percent_fact_income'] = (grand_totals['fact_income'] / grand_totals['plan_income'] * 100) if \
        grand_totals['plan_income'] > 0 else 0

    return grand_totals