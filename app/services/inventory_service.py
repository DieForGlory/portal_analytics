# app/services/inventory_service.py

from collections import defaultdict
from ..core.db_utils import get_mysql_session, get_planning_session, get_default_session
import pandas as pd
import io
from app.models.planning_models import map_mysql_key_to_russian_value
from app.models.planning_models import DiscountVersion, PaymentMethod, PropertyType
from app.models.estate_models import EstateSell, EstateHouse
from app.models.finance_models import FinanceOperation
from app.models.exclusion_models import ExcludedComplex
from app.core.extensions import db
from sqlalchemy import func


def generate_commercial_inventory_excel(currency: str, usd_rate: float):
    """
    Генерирует Excel-файл для коммерческой недвижимости с разбивкой по этажам и ЦЕНАМИ ДНА.
    """
    mysql_session = get_mysql_session()
    planning_session = get_planning_session() # Открываем сессию для доступа к скидкам
    is_usd = currency == 'USD'
    rate = usd_rate if is_usd else 1.0

    # 1. Получаем активную версию скидок (100% оплата)
    active_version = planning_session.query(DiscountVersion).filter_by(is_active=True).first()
    discounts_map = {}
    if active_version:
        discounts_map = {
            (d.complex_name, d.property_type): d
            for d in active_version.discounts
            if d.payment_method == PaymentMethod.FULL_PAYMENT
        }

    # 2. Запрос коммерческих объектов в продаже
    commercial_sells = mysql_session.query(EstateSell).options(
        db.joinedload(EstateSell.house)
    ).filter(
        EstateSell.estate_sell_status_name.in_(["Маркетинговый резерв", "Подбор", "Бронь"]),
        EstateSell.estate_sell_category == 'comm'
    ).all()

    data_rows = []
    for sell in commercial_sells:
        if not sell.house:
            continue

        # Определяем тип недвижимости для поиска скидки
        try:
            rus_cat = map_mysql_key_to_russian_value(sell.estate_sell_category)
            prop_type_enum = PropertyType(rus_cat)
        except ValueError:
            continue

        # РАСЧЕТ ЦЕНЫ ДНА (Floor Price)
        discount = discounts_map.get((sell.house.complex_name, prop_type_enum))
        bottom_price = 0
        if sell.estate_price:
            price_for_calc = sell.estate_price # Для коммерции вычета 3 млн нет
            if price_for_calc > 0 and discount:
                # Суммируем коэффициенты МПП, РОП и КД
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0)
                bottom_price = price_for_calc * (1 - total_discount_rate)
            else:
                bottom_price = price_for_calc

        # Категория этажа
        try:
            floor = int(sell.estate_floor)
        except (ValueError, TypeError):
            floor = 1
        floor_cat = "1-й этаж" if floor == 1 else ("Подвальный / Цокольный" if floor <= 0 else f"{floor}-й этаж")

        data_rows.append({
            'Проект': sell.house.complex_name,
            'Этаж': floor_cat,
            'ID объекта': sell.id,
            'Площадь, м²': sell.estate_area,
            f'Цена дна за м² ({currency})': (bottom_price / sell.estate_area / rate) if sell.estate_area else 0,
            f'Общая стоимость дна ({currency})': (bottom_price / rate),
            'Статус': sell.estate_sell_status_name
        })

    planning_session.close()

    if not data_rows:
        return None

    # Создание DataFrame и экспорт в Excel
    df = pd.DataFrame(data_rows).sort_values(by=['Проект', 'Этаж'])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Коммерция (дно)')
        workbook = writer.book
        worksheet = writer.sheets['Коммерция (дно)']

        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        money_fmt = workbook.add_format({'num_format': '"$"#,##0.00' if is_usd else '#,##0', 'border': 1})
        area_fmt = workbook.add_format({'num_format': '#,##0.00', 'border': 1})

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_fmt)

        worksheet.set_column('A:B', 25)
        worksheet.set_column('D:D', 15, area_fmt)
        worksheet.set_column('E:F', 22, money_fmt)
        worksheet.set_column('G:G', 20)

    output.seek(0)
    return output

def get_inventory_summary_data():
    default_session = get_default_session()
    planning_session = get_planning_session()
    mysql_session = get_mysql_session()
    """
    Собирает данные по остаткам и возвращает детализацию и общую сводку,
    учитывая исключенные ЖК. Также включает общую сумму ожидаемых поступлений.
    Возвращает: summary_by_complex, overall_summary, summary_by_house
    """
    # 1. Получаем список исключенных ЖК
    excluded_complex_names = {c.complex_name for c in default_session.query(ExcludedComplex).all()}

    # 2. Получаем активную версию скидок
    active_version = planning_session.query(DiscountVersion).filter_by(is_active=True).first()
    discounts_map = {}
    if active_version:
        discounts_map = {
            (d.complex_name, d.property_type): d
            for d in active_version.discounts
            if d.payment_method == PaymentMethod.FULL_PAYMENT
        }

    # 3. Получаем остатки (Unsold items)
    valid_statuses = ["Маркетинговый резерв", "Подбор", "Бронь"]
    unsold_sells_query = mysql_session.query(EstateSell).options(
        db.joinedload(EstateSell.house)
    ).filter(
        EstateSell.estate_sell_status_name.in_(valid_statuses),
        EstateSell.estate_price.isnot(None),
        EstateSell.estate_area > 0
    )

    if excluded_complex_names:
        unsold_sells_query = unsold_sells_query.join(EstateSell.house).filter(
            EstateHouse.complex_name.notin_(excluded_complex_names)
        )

    unsold_sells = unsold_sells_query.all()

    # 4. Получаем все ожидаемые поступления (с разбивкой по домам)
    expected_income_query = mysql_session.query(
        EstateHouse.complex_name,
        EstateHouse.name,  # Добавляем имя дома в выборку
        EstateSell.estate_sell_category,
        func.sum(FinanceOperation.summa)
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        FinanceOperation.status_name == "К оплате",
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки"
    )

    if excluded_complex_names:
        expected_income_query = expected_income_query.filter(
            EstateHouse.complex_name.notin_(excluded_complex_names)
        )

    expected_income_results = expected_income_query.group_by(
        EstateHouse.complex_name, EstateHouse.name, EstateSell.estate_sell_category
    ).all()

    # Собираем мапу ожидаемых поступлений: (ЖК, Дом, Тип) -> Сумма
    expected_income_map = defaultdict(float)
    for complex_name, house_name, cat_key, amount in expected_income_results:
        try:
            rus_val = map_mysql_key_to_russian_value(cat_key)
            expected_income_map[(complex_name, house_name, rus_val)] += (amount or 0)
        except ValueError:
            continue

    # 5. Агрегируем данные по ДОМАМ (более детально)
    summary_by_house = defaultdict(lambda: defaultdict(lambda: {
        'units': 0, 'total_area': 0.0, 'total_value': 0.0, 'expected_income': 0.0
    }))

    # Обрабатываем остатки
    for sell in unsold_sells:
        if not sell.house:
            continue
        try:
            russian_category_value = map_mysql_key_to_russian_value(sell.estate_sell_category)
            prop_type_enum = PropertyType(russian_category_value)
            complex_name = sell.house.complex_name
            house_name = sell.house.name
            if not complex_name:
                continue
        except ValueError:
            continue

        discount = discounts_map.get((complex_name, prop_type_enum))
        bottom_price = 0
        if sell.estate_price:
            deduction = 3_000_000 if prop_type_enum == PropertyType.FLAT else 0
            price_for_calc = sell.estate_price - deduction
            if price_for_calc > 0 and discount:
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0)
                bottom_price = price_for_calc * (1 - total_discount_rate)
            elif price_for_calc > 0:
                bottom_price = price_for_calc

        # Ключ теперь включает дом
        metrics = summary_by_house[(complex_name, house_name)][russian_category_value]
        metrics['units'] += 1
        metrics['total_area'] += sell.estate_area
        metrics['total_value'] += bottom_price

    # Добавляем данные по ожидаемым поступлениям в разбивку по домам
    for (c_name, h_name, p_type), amount in expected_income_map.items():
        summary_by_house[(c_name, h_name)][p_type]['expected_income'] = amount

    # 6. Агрегируем данные по ЖК (для совместимости с веб-вью)
    summary_by_complex = defaultdict(lambda: defaultdict(lambda: {
        'units': 0, 'total_area': 0.0, 'total_value': 0.0, 'expected_income': 0.0
    }))

    for (c_name, h_name), prop_data in summary_by_house.items():
        for p_type, metrics in prop_data.items():
            target = summary_by_complex[c_name][p_type]
            target['units'] += metrics['units']
            target['total_area'] += metrics['total_area']
            target['total_value'] += metrics['total_value']
            target['expected_income'] += metrics['expected_income']

    # 7. Считаем общую сводку
    overall_summary = defaultdict(lambda: {
        'units': 0, 'total_area': 0.0, 'total_value': 0.0, 'expected_income': 0.0
    })

    for prop_types_data in summary_by_complex.values():
        for prop_type, metrics in prop_types_data.items():
            overall_summary[prop_type]['units'] += metrics['units']
            overall_summary[prop_type]['total_area'] += metrics['total_area']
            overall_summary[prop_type]['total_value'] += metrics['total_value']
            overall_summary[prop_type]['expected_income'] += metrics['expected_income']

    # 8. Считаем средние цены
    # Для сводки по домам
    for key, prop_types_data in summary_by_house.items():
        for prop_type, metrics in prop_types_data.items():
            if metrics['total_area'] > 0:
                metrics['avg_price_m2'] = metrics['total_value'] / metrics['total_area']
            else:
                metrics['avg_price_m2'] = 0

    # Для сводки по ЖК
    for complex_name, prop_types_data in summary_by_complex.items():
        for prop_type, metrics in prop_types_data.items():
            if metrics['total_area'] > 0:
                metrics['avg_price_m2'] = metrics['total_value'] / metrics['total_area']
            else:
                metrics['avg_price_m2'] = 0

    # Для общей сводки
    for prop_type, metrics in overall_summary.items():
        if metrics['total_area'] > 0:
            metrics['avg_price_m2'] = metrics['total_value'] / metrics['total_area']
        else:
            metrics['avg_price_m2'] = 0

    return summary_by_complex, overall_summary, summary_by_house


def generate_inventory_excel(summary_data: dict, currency: str, usd_rate: float):
    """
    Создает Excel-файл. Поддерживает как группировку по ЖК, так и по (ЖК, Дом).
    """
    flat_data = []
    is_usd = currency == 'USD'
    rate = usd_rate if is_usd else 1.0
    currency_suffix = f', {currency}'

    value_header = 'Стоимость остатков (дно)' + currency_suffix
    price_header = 'Цена дна, за м²' + currency_suffix
    expected_header = 'Ожидаемые поступления' + currency_suffix

    for key, prop_types_data in summary_data.items():
        # Определяем ключи
        if isinstance(key, tuple):
            complex_name, house_name = key
        else:
            complex_name = key
            house_name = "-"

        for prop_type, metrics in prop_types_data.items():
            row = {
                'Проект': complex_name,
                'Тип недвижимости': prop_type,
                'Остаток, шт.': metrics['units'],
                'Остаток, м²': metrics['total_area'],
                value_header: metrics['total_value'] / rate,
                price_header: metrics['avg_price_m2'] / rate,
                expected_header: metrics.get('expected_income', 0) / rate
            }
            # Если есть данные по дому, добавляем в строку
            if house_name != "-":
                row['Дом'] = house_name
            flat_data.append(row)

    if not flat_data:
        return None

    df = pd.DataFrame(flat_data)

    # Упорядочиваем колонки, чтобы Дом был после Проекта
    cols = list(df.columns)
    if 'Дом' in cols:
        cols.insert(1, cols.pop(cols.index('Дом')))
        df = df[cols]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Сводка по остаткам', startrow=1, header=False)
        workbook = writer.book
        worksheet = writer.sheets['Сводка по остаткам']

        header_format = workbook.add_format(
            {'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1})
        money_format_uzs = workbook.add_format({'num_format': '#,##0', 'border': 1})
        money_format_usd = workbook.add_format({'num_format': '"$"#,##0.00', 'border': 1})
        area_format = workbook.add_format({'num_format': '#,##0.00', 'border': 1})
        integer_format = workbook.add_format({'num_format': '0', 'border': 1})
        default_format = workbook.add_format({'border': 1})

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        current_money_format = money_format_usd if is_usd else money_format_uzs

        # Настройка ширины колонок (с учетом сдвига из-за новой колонки)
        col_idx = 0
        worksheet.set_column(col_idx, col_idx, 25, default_format)  # Проект
        col_idx += 1

        if 'Дом' in df.columns:
            worksheet.set_column(col_idx, col_idx, 15, default_format)  # Дом
            col_idx += 1

        worksheet.set_column(col_idx, col_idx, 25, default_format)  # Тип
        col_idx += 1
        worksheet.set_column(col_idx, col_idx, 15, integer_format)  # Остаток шт
        col_idx += 1
        worksheet.set_column(col_idx, col_idx, 15, area_format)  # Остаток м2
        col_idx += 1
        worksheet.set_column(col_idx, col_idx, 30, current_money_format)  # Стоимость
        col_idx += 1
        worksheet.set_column(col_idx, col_idx, 25, current_money_format)  # Цена м2
        col_idx += 1
        worksheet.set_column(col_idx, col_idx, 30, current_money_format)  # Ожидаемые

    output.seek(0)
    return output