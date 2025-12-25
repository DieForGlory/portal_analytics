# app/services/pricelist_service.py
import io
import pandas as pd
from datetime import date
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from app.core.db_utils import get_planning_session, get_mysql_session
from app.models import planning_models
from app.models.estate_models import EstateSell, EstateHouse
from app.models.planning_models import map_russian_to_mysql_key, PropertyType
from app.services import currency_service

RESERVATION_FEE = 3_000_000
REMAINDER_STATUSES = ["Маркетинговый резерв", "Подбор"]

def calculate_new_prices(complex_name, property_type_ru, percent_change):
    mysql_session = get_mysql_session()
    planning_session = get_planning_session()
    try:
        usd_rate = currency_service.get_current_effective_rate() or 12800.0
        prop_type_enum = PropertyType(property_type_ru)
        mysql_key = map_russian_to_mysql_key(property_type_ru)

        active_version = planning_session.query(planning_models.DiscountVersion).filter_by(is_active=True).first()
        if not active_version:
            return None, "Нет активной версии скидок"

        discount = planning_session.query(planning_models.Discount).filter_by(
            version_id=active_version.id,
            complex_name=complex_name,
            property_type=prop_type_enum,
            payment_method=planning_models.PaymentMethod.FULL_PAYMENT
        ).first()

        total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0) if discount else 0
        discount_multiplier = 1 - total_discount_rate

        house_ids = [h.id for h in mysql_session.query(EstateHouse.id).filter_by(complex_name=complex_name).all()]
        all_objects = mysql_session.query(EstateSell).filter(
            EstateSell.house_id.in_(house_ids),
            EstateSell.estate_sell_category == mysql_key
        ).all()

        excel_results = []
        stats = {
            'complex_name': complex_name,
            'prop_type': property_type_ru,
            'total_units_project': len(all_objects),
            'remainder_units': 0,
            'remainder_sqm': 0.0,
            'discount_pct': round(total_discount_rate * 100, 2),
            'percent_change': round(percent_change * 100, 2),
            'floor_prices_before': [], 'floor_prices_after': [],
            'floor_totals_before': [], 'floor_totals_after': [],
            'final_totals_before': 0.0, 'final_totals_after': 0.0,
            'usd_rate': usd_rate
        }

        for obj in all_objects:
            if not obj.estate_price or not obj.estate_area or obj.estate_area <= 0: continue

            c_floor_total_uzs = (obj.estate_price - RESERVATION_FEE) * discount_multiplier
            c_floor_sqm_usd = (c_floor_total_uzs / obj.estate_area) / usd_rate
            n_floor_sqm_usd = c_floor_sqm_usd * (1 + percent_change)
            n_floor_total_uzs = (n_floor_sqm_usd * usd_rate) * obj.estate_area
            n_estate_price = (n_floor_total_uzs / discount_multiplier) + RESERVATION_FEE

            excel_results.append({'Id обьекта': obj.id, 'Новая стоимость': round(n_estate_price, -3)})

            if obj.estate_sell_status_name in REMAINDER_STATUSES:
                stats['remainder_units'] += 1
                stats['remainder_sqm'] += obj.estate_area
                stats['floor_prices_before'].append(c_floor_sqm_usd)
                stats['floor_prices_after'].append(n_floor_sqm_usd)
                stats['floor_totals_before'].append(c_floor_total_uzs / usd_rate)
                stats['floor_totals_after'].append(n_floor_total_uzs / usd_rate)
                stats['final_totals_before'] += obj.estate_price / usd_rate
                stats['final_totals_after'] += n_estate_price / usd_rate

        return excel_results, stats
    finally:
        mysql_session.close()
        planning_session.close()

def _set_border(ws, cell_range):
    thin = Side(border_style="thin", color="000000")
    for row in ws[cell_range]:
        for cell in row:
            cell.border = Border(top=thin, left=thin, right=thin, bottom=thin)


def generate_pricelist_excel(results, stats):
    output = io.BytesIO()
    wb = Workbook()

    # Лист 1: Реестр (технический)
    ws1 = wb.active
    ws1.title = "PriceList"
    ws1.append(["Id обьекта", "Новая стоимость"])
    for res in results:
        ws1.append([res['Id обьекта'], res['Новая стоимость']])

    # Лист 2: На_подпись (форматированный)
    ws = wb.create_sheet("На_подпись")

    # Стили из шаблона
    f_header = Font(bold=True, name='Arial', size=11)
    f_bold = Font(bold=True, name='Arial', size=10)
    a_center = Alignment(horizontal='center', vertical='center')
    a_left = Alignment(horizontal='left', vertical='center', indent=1)

    # Цвета (заливка)
    fill_gray = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    fill_yellow = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

    # Настройка ширины колонок
    ws.column_dimensions['A'].width = 2
    ws.column_dimensions['B'].width = 45
    ws.column_dimensions['C'].width = 18
    ws.column_dimensions['D'].width = 18

    # 1. Метаданные (Строки 2-9)
    metadata = [
        ("Проект", stats['complex_name']),
        ("Тип недвижимости", stats['prop_type']),
        ("Всего квартир в проекте, шт.", stats['total_units_project']),
        ("Всего товарного запаса, шт", stats['remainder_units']),
        ("Всего товарного запаса, кв.м", round(stats['remainder_sqm'], 2)),
        ("Сумма регламентных скидок, %", f"{stats['discount_pct']}%"),
        ("Средняя площадь товарного запаса, кв.м",
         round(stats['remainder_sqm'] / stats['remainder_units'], 2) if stats['remainder_units'] > 0 else 0),
        ("Процент повышения цены дна, %", f"{stats['percent_change']}%")
    ]

    for i, (label, value) in enumerate(metadata, 2):
        cell_l = ws.cell(row=i, column=2, value=label)
        cell_v = ws.cell(row=i, column=3, value=value)
        cell_l.font = f_bold
        cell_l.fill = fill_gray
        _set_border(ws, f'B{i}:C{i}')

    # 2. Раздел: Изменение цены (Строка 11)
    ws.merge_cells('B11:D11')
    hdr = ws['B11']
    hdr.value = "Изменение цены"
    hdr.font = f_header
    hdr.alignment = a_center
    hdr.fill = fill_yellow
    _set_border(ws, 'B11:D11')

    # Заголовки таблицы сравнения (Строка 13)
    ws['C13'], ws['D13'] = "Было", "Стало"
    for col in [3, 4]:
        cell = ws.cell(row=13, column=col)
        cell.font = f_bold
        cell.alignment = a_center
        cell.fill = fill_gray
    _set_border(ws, 'C13:D13')

    # 3. Блок: Цена дна, $ (Строки 14-16)
    price_metrics = [
        ("Минимальная цена дна, $", stats['floor_prices_before'], stats['floor_prices_after']),
        ("Средняя цена дна, $", stats['floor_prices_before'], stats['floor_prices_after']),
        ("Максимальная цена дна, $", stats['floor_prices_before'], stats['floor_prices_after'])
    ]

    for i, (label, before, after) in enumerate(price_metrics):
        row_idx = 14 + i
        ws.cell(row=row_idx, column=2, value=label).font = f_bold
        if before:
            vals = [min(before), sum(before) / len(before), max(before)]
            n_vals = [min(after), sum(after) / len(after), max(after)]
            ws.cell(row=row_idx, column=3, value=round(vals[i], 2))
            ws.cell(row=row_idx, column=4, value=round(n_vals[i], 2))
        _set_border(ws, f'B{row_idx}:D{row_idx}')

    # 4. Блок: Стоимость дна, $ (Строки 18-20)
    cost_metrics = [
        ("Минимальная стоимость дна, $", stats['floor_totals_before'], stats['floor_totals_after']),
        ("Средняя стоимость дна, $", stats['floor_totals_before'], stats['floor_totals_after']),
        ("Максимальная стоимость дна, $", stats['floor_totals_before'], stats['floor_totals_after'])
    ]

    for i, (label, before, after) in enumerate(cost_metrics):
        row_idx = 18 + i
        ws.cell(row=row_idx, column=2, value=label).font = f_bold
        if before:
            vals = [min(before), sum(before) / len(before), max(before)]
            n_vals = [min(after), sum(after) / len(after), max(after)]
            ws.cell(row=row_idx, column=3, value=round(vals[i], 0))
            ws.cell(row=row_idx, column=4, value=round(n_vals[i], 0))
        _set_border(ws, f'B{row_idx}:D{row_idx}')

    # 5. Итого (Строка 22)
    ws.cell(row=22, column=2, value="Общая стоимость остатков, $").font = f_header
    ws.cell(row=22, column=3, value=round(stats['final_totals_before'], 0))
    ws.cell(row=22, column=4, value=round(stats['final_totals_after'], 0))
    _set_border(ws, 'B22:D22')

    wb.save(output)
    output.seek(0)
    return output