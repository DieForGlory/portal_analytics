# app/services/report_service.py
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sqlalchemy import func, extract
from ..core.db_utils import get_planning_session, get_mysql_session
import io
from collections import defaultdict

# --- ИЗМЕНЕНИЯ ЗДЕСЬ: Обновляем импорты ---
from app.models import planning_models
from .data_service import get_all_complex_names
from ..models.estate_models import EstateDeal, EstateHouse, EstateSell
from ..models.finance_models import FinanceOperation
from . import currency_service

# --- ДОБАВЛЕНЫ НУЖНЫЕ ИМПОРТЫ "ПЕРЕВОДЧИКОВ" ---
from app.models.planning_models import map_russian_to_mysql_key, map_mysql_key_to_russian_value, PropertyType, \
    PaymentMethod


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
    Генерирует сводный отчет за период (квартал, полугодие), суммируя данные по месяцам.
    """
    # (Код этой функции без изменений, т.к. она вызывает generate_plan_fact_report)
    PERIOD_MONTHS = {
        'q1': range(1, 4),  # 1-й квартал
        'q2': range(4, 7),  # 2-й квартал
        'q3': range(7, 10),  # 3-й квартал
        'q4': range(10, 13),  # 4-й квартал
        'h1': range(1, 7),  # 1-е полугодие
        'h2': range(7, 13),  # 2-е полугодие
    }

    months_in_period = PERIOD_MONTHS.get(period)
    if not months_in_period:
        return [], {}

    aggregated_data = defaultdict(lambda: defaultdict(float))
    aggregated_totals = defaultdict(float)

    for month in months_in_period:
        monthly_data, monthly_totals, _ = generate_plan_fact_report(year, month, property_type)  # Игнорируем refunds

        for project_row in monthly_data:
            complex_name = project_row['complex_name']
            for key, value in project_row.items():
                if key != 'complex_name' and isinstance(value, (int, float)):
                    aggregated_data[complex_name][key] += value
            aggregated_data[complex_name]['complex_name'] = complex_name

        for key, value in monthly_totals.items():
            if isinstance(value, (int, float)):
                aggregated_totals[key] += value

    final_report_data = []
    for complex_name, data in aggregated_data.items():
        data['percent_fact_units'] = (data['fact_units'] / data['plan_units'] * 100) if data['plan_units'] > 0 else 0
        data['percent_fact_volume'] = (data['fact_volume'] / data['plan_volume'] * 100) if data[
                                                                                               'plan_volume'] > 0 else 0
        data['percent_fact_income'] = (data['fact_income'] / data['plan_income'] * 100) if data[
                                                                                               'plan_income'] > 0 else 0
        data['forecast_units'] = 0
        data['forecast_volume'] = 0
        final_report_data.append(dict(data))

    aggregated_totals['percent_fact_units'] = (
            aggregated_totals['fact_units'] / aggregated_totals['plan_units'] * 100) if aggregated_totals[
                                                                                            'plan_units'] > 0 else 0
    aggregated_totals['percent_fact_volume'] = (
            aggregated_totals['fact_volume'] / aggregated_totals['plan_volume'] * 100) if aggregated_totals[
                                                                                              'plan_volume'] > 0 else 0
    aggregated_totals['percent_fact_income'] = (
            aggregated_totals['fact_income'] / aggregated_totals['plan_income'] * 100) if aggregated_totals[
                                                                                              'plan_income'] > 0 else 0
    aggregated_totals['forecast_units'] = 0
    aggregated_totals['forecast_volume'] = 0

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


def generate_plan_fact_excel(year: int, month: int, property_type: str):
    """
    Генерирует Excel-файл с детальным план-фактным отчетом (ИСПРАВЛЕННАЯ ВЕРСИЯ).
    """
    # (Код этой функции без изменений)
    report_data, totals, total_refunds = generate_plan_fact_report(year, month, property_type)

    if not report_data:
        return None

    df = pd.DataFrame(report_data)

    # --- ИСПРАВЛЕНИЕ: Извлекаем 'sum' из 'expected_income' ---
    # Убедимся, что 'expected_income' в totals также является числом
    if 'expected_income' in totals and isinstance(totals['expected_income'], dict):
        totals['expected_income'] = totals['expected_income'].get('sum', 0)

    totals_df = pd.DataFrame([totals])
    # ---

    ordered_columns = [
        'complex_name',
        'plan_units', 'fact_units', 'percent_fact_units', 'forecast_units',
        'plan_volume', 'fact_volume', 'percent_fact_volume', 'forecast_volume',
        'plan_income', 'fact_income', 'percent_fact_income', 'expected_income'
    ]
    renamed_columns = [
        'Проект',
        'План, шт', 'Факт, шт', '% Факт, шт', '% Прогноз, шт',
        'План контрактации', 'Факт контрактации', '% Факт контр.', '% Прогноз контр.',
        'План поступлений', 'Факт поступлений', '% Факт поступл.', 'Ожидаемые поступл.'
    ]

    # --- ИСПРАВЛЕНИЕ: Извлекаем 'sum' из 'expected_income' ---
    df['expected_income'] = df['expected_income'].apply(lambda x: x.get('sum', 0) if isinstance(x, dict) else 0)
    # ---

    df = df[ordered_columns]
    totals_df = totals_df[[col for col in ordered_columns if col != 'complex_name']]
    totals_df.insert(0, 'complex_name', f'Итого ({property_type})')

    final_df = pd.concat([df, totals_df], ignore_index=True)
    final_df.columns = renamed_columns

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name=f'План-факт {month:02d}-{year}')

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