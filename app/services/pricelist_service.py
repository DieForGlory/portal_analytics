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
        'floor_prices_before': [],
        'floor_prices_after': [],
        'floor_totals_before': [],
        'floor_totals_after': [],
        'final_totals_before': 0.0,
        'final_totals_after': 0.0,
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

    mysql_session.close();
    planning_session.close()
    return excel_results, stats


def _set_border(ws, cell_range):
    """Применяет границы ко всем ячейкам в диапазоне (включая объединенные)."""
    thin = Side(border_style="thin", color="000000")
    for row in ws[cell_range]:
        for cell in row:
            cell.border = Border(top=thin, left=thin, right=thin, bottom=thin)


def generate_pricelist_excel(results, stats):
    output = io.BytesIO();
    wb = Workbook()

    # Лист 1: Реестр
    ws1 = wb.active;
    ws1.title = "PriceList"
    ws1.append(["Id обьекта", "Новая стоимость"])
    for res in results: ws1.append([res['Id обьекта'], res['Новая стоимость']])

    # Лист 2: На_подпись
    ws = wb.create_sheet("На_подпись")

    # Стили
    f_bold = Font(bold=True, name='Arial', size=10)
    f_h = Font(bold=True, name='Arial', size=11)
    a_c = Alignment(horizontal='center', vertical='center', wrap_text=True)
    fill_g = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
    fill_y = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

    ws.column_dimensions['A'].width = 2
    ws.column_dimensions['B'].width = 40
    for c in ['C', 'D', 'E', 'F', 'G']: ws.column_dimensions[c].width = 24

    # Секция 1: Метаданные
    rows = [
        ("Наименование проекта", stats['complex_name']),
        ("Тип недвижимости в прайсе", stats['prop_type']),
        ("Всего обьектов в проекте", stats['total_units_project']),
        ("Всего товарных запасов, шт", stats['remainder_units']),
        ("Всего товарных запасов, кв.м", round(stats['remainder_sqm'], 2)),
        ("Сумма регламентных скидок, %", f"{stats['discount_pct']}%"),
        ("Средняя площадь товарных запасов",
         round(stats['remainder_sqm'] / stats['remainder_units'], 2) if stats['remainder_units'] > 0 else 0),
        ("Процент повышения цены дна,%", f"{stats['percent_change']}%")
    ]
    for i, (l, v) in enumerate(rows, 1):
        ws.cell(row=i, column=2, value=l).font = f_bold
        ws.cell(row=i, column=3, value=v)
        _set_border(ws, f'B{i}:C{i}')

    # Секция 2: Цена дна
    ws.merge_cells('B10:G10')
    c_h1 = ws['B10'];
    c_h1.value = "Цена дна товарных запасов, $";
    c_h1.font = f_h;
    c_h1.alignment = a_c;
    c_h1.fill = fill_g
    _set_border(ws, 'B10:G10')

    ws.merge_cells('B11:D11');
    ws['B11'].value = "Было"
    ws.merge_cells('E11:G11');
    ws['E11'].value = "Стало"
    for cell in [ws['B11'], ws['E11']]:
        cell.font = f_bold;
        cell.alignment = a_c;
        cell.fill = fill_y
    _set_border(ws, 'B11:G11')

    s_h = ["Минимальная цена дна, $", "Средняя цена дна, $", "Максимальная цена дна, $"]
    for i, t in enumerate(s_h + s_h, 2):
        cell = ws.cell(row=12, column=i, value=t)
        cell.font = f_bold;
        cell.alignment = a_c;
        cell.fill = fill_g;
        cell.border = Border(top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'),
                             bottom=Side(style='thin'))

    f_b, f_a = stats['floor_prices_before'], stats['floor_prices_after']
    if f_b:
        vals = [min(f_b), sum(f_b) / len(f_b), max(f_b), min(f_a), sum(f_a) / len(f_a), max(f_a)]
        for i, v in enumerate(vals, 2):
            cell = ws.cell(row=13, column=i, value=round(v, 2));
            cell.alignment = a_c;
            cell.border = Border(top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'),
                                 bottom=Side(style='thin'))

    # Секция 3: Стоимость дна
    ws.merge_cells('B15:G15')
    c_h2 = ws['B15'];
    c_h2.value = "Стоимость дна товарных запасов, $";
    c_h2.font = f_h;
    c_h2.alignment = a_c;
    c_h2.fill = fill_g
    _set_border(ws, 'B15:G15')

    ws.merge_cells('B16:D16');
    ws['B16'].value = "Было"
    ws.merge_cells('E16:G16');
    ws['E16'].value = "Стало"
    for cell in [ws['B16'], ws['E16']]:
        cell.font = f_bold;
        cell.alignment = a_c;
        cell.fill = fill_y
    _set_border(ws, 'B16:G16')

    s_c = ["Минимальная стоимость дна, $", "Средняя стоимость дна, $", "Максимальная стоимость дна, $"]
    for i, t in enumerate(s_c + s_c, 2):
        cell = ws.cell(row=17, column=i, value=t)
        cell.font = f_bold;
        cell.alignment = a_c;
        cell.fill = fill_g;
        cell.border = Border(top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'),
                             bottom=Side(style='thin'))

    t_b, t_a = stats['floor_totals_before'], stats['floor_totals_after']
    if t_b:
        vals = [min(t_b), sum(t_b) / len(t_b), max(t_b), min(t_a), sum(t_a) / len(t_a), max(t_a)]
        for i, v in enumerate(vals, 2):
            cell = ws.cell(row=18, column=i, value=round(v, 0));
            cell.alignment = a_c;
            cell.border = Border(top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'),
                                 bottom=Side(style='thin'))

    # Секция 4: Итого
    ws.merge_cells('B20:G20')
    c_h3 = ws['B20'];
    c_h3.value = "Стоимость товарных запасов, $ млн ";
    c_h3.font = f_h;
    c_h3.alignment = a_c;
    c_h3.fill = fill_g
    _set_border(ws, 'B20:G20')

    ws.merge_cells('B21:D21');
    ws['B21'].value = "Было, $"
    ws.merge_cells('E21:G21');
    ws['E21'].value = "Стало, $"
    for cell in [ws['B21'], ws['E21']]: cell.font = f_bold; cell.alignment = a_c; cell.border = Border(
        top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'), bottom=Side(style='thin'))
    _set_border(ws, 'B21:G21')

    ws.merge_cells('B22:D22');
    ws['B22'].value = round(stats['final_totals_before'] / 1e6, 2)
    ws.merge_cells('E22:G22');
    ws['E22'].value = round(stats['final_totals_after'] / 1e6, 2)
    for cell in [ws['B22'], ws['E22']]: cell.font = f_h; cell.alignment = a_c; cell.border = Border(
        top=Side(style='thin'), left=Side(style='thin'), right=Side(style='thin'), bottom=Side(style='thin'))
    _set_border(ws, 'B22:G22')

    wb.save(output);
    output.seek(0);
    return output