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


def get_inventory_summary_data():
    default_session = get_default_session()
    planning_session = get_planning_session()
    mysql_session = get_mysql_session()
    """
    Собирает данные по остаткам и возвращает детализацию и общую сводку,
    учитывая исключенные ЖК. Также включает общую сумму ожидаемых поступлений.
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

    # 4. Получаем все ожидаемые поступления (без фильтра по дате)
    expected_income_query = mysql_session.query(
        EstateHouse.complex_name,
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
        EstateHouse.complex_name, EstateSell.estate_sell_category
    ).all()

    # Собираем мапу ожидаемых поступлений: (ЖК, Тип) -> Сумма
    expected_income_map = defaultdict(float)
    for complex_name, cat_key, amount in expected_income_results:
        try:
            rus_val = map_mysql_key_to_russian_value(cat_key)
            expected_income_map[(complex_name, rus_val)] += (amount or 0)
        except ValueError:
            continue

    # 5. Агрегируем данные
    summary_by_complex = defaultdict(lambda: defaultdict(lambda: {
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

        metrics = summary_by_complex[complex_name][russian_category_value]
        metrics['units'] += 1
        metrics['total_area'] += sell.estate_area
        metrics['total_value'] += bottom_price

    # Добавляем данные по ожидаемым поступлениям
    for (c_name, p_type), amount in expected_income_map.items():
        summary_by_complex[c_name][p_type]['expected_income'] = amount

    # 6. Считаем общую сводку
    overall_summary = defaultdict(lambda: {
        'units': 0, 'total_area': 0.0, 'total_value': 0.0, 'expected_income': 0.0
    })

    for prop_types_data in summary_by_complex.values():
        for prop_type, metrics in prop_types_data.items():
            overall_summary[prop_type]['units'] += metrics['units']
            overall_summary[prop_type]['total_area'] += metrics['total_area']
            overall_summary[prop_type]['total_value'] += metrics['total_value']
            overall_summary[prop_type]['expected_income'] += metrics['expected_income']

    # 7. Считаем средние цены
    for complex_name, prop_types_data in summary_by_complex.items():
        for prop_type, metrics in prop_types_data.items():
            if metrics['total_area'] > 0:
                metrics['avg_price_m2'] = metrics['total_value'] / metrics['total_area']
            else:
                metrics['avg_price_m2'] = 0

    for prop_type, metrics in overall_summary.items():
        if metrics['total_area'] > 0:
            metrics['avg_price_m2'] = metrics['total_value'] / metrics['total_area']
        else:
            metrics['avg_price_m2'] = 0

    return summary_by_complex, overall_summary


def generate_inventory_excel(summary_data: dict, currency: str, usd_rate: float):
    """
    Создает Excel-файл с учетом выбранной валюты и новым столбцом ожидаемых поступлений.
    """
    flat_data = []
    is_usd = currency == 'USD'
    rate = usd_rate if is_usd else 1.0
    currency_suffix = f', {currency}'

    value_header = 'Стоимость остатков (дно)' + currency_suffix
    price_header = 'Цена дна, за м²' + currency_suffix
    expected_header = 'Ожидаемые поступления' + currency_suffix

    for complex_name, prop_types_data in summary_data.items():
        for prop_type, metrics in prop_types_data.items():
            flat_data.append({
                'Проект': complex_name,
                'Тип недвижимости': prop_type,
                'Остаток, шт.': metrics['units'],
                'Остаток, м²': metrics['total_area'],
                value_header: metrics['total_value'] / rate,
                price_header: metrics['avg_price_m2'] / rate,
                expected_header: metrics.get('expected_income', 0) / rate
            })

    if not flat_data:
        return None

    df = pd.DataFrame(flat_data)
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

        # Настройка ширины колонок
        worksheet.set_column(0, 0, 25, default_format)  # Проект
        worksheet.set_column(1, 1, 25, default_format)  # Тип
        worksheet.set_column(2, 2, 15, integer_format)  # Остаток шт
        worksheet.set_column(3, 3, 15, area_format)  # Остаток м2
        worksheet.set_column(4, 4, 30, current_money_format)  # Стоимость
        worksheet.set_column(5, 5, 25, current_money_format)  # Цена м2
        worksheet.set_column(6, 6, 30, current_money_format)  # Ожидаемые поступления (новый столбец)

    output.seek(0)
    return output