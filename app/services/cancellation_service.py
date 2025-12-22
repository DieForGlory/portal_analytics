# app/services/cancellation_service.py

from datetime import datetime
from app.core.db_utils import get_default_session, get_mysql_session
from app.models.registry_models import CancellationRegistry
from app.models.estate_models import EstateSell, EstateHouse, EstateDeal
from sqlalchemy import desc
from sqlalchemy.orm import joinedload


def get_cancellations():
    """
    Возвращает список расторжений.
    Логика: Данные берутся из MySQL. Если там отсутствуют номер/дата/сумма,
    проверяются поля ручного ввода (manual_*) из локальной базы.
    """
    default_session = get_default_session()
    mysql_session = get_mysql_session()

    # 1. Получаем список расторжений из локальной базы
    cancellations = default_session.query(CancellationRegistry).order_by(desc(CancellationRegistry.created_at)).all()

    if not cancellations:
        return []

    canc_map = {c.estate_sell_id: c for c in cancellations}
    sell_ids = list(canc_map.keys())

    # 2. Загружаем данные объектов из MySQL
    sells = mysql_session.query(EstateSell).options(
        joinedload(EstateSell.house),
        joinedload(EstateSell.deals)
    ).filter(
        EstateSell.id.in_(sell_ids)
    ).all()

    # Создаем словарь для быстрого поиска sell по id
    sells_map = {s.id: s for s in sells}

    result = []

    # Итерируемся по записям реестра, чтобы не потерять те, которых нет в MySQL (хотя такое редкость)
    for canc in cancellations:
        sell = sells_map.get(canc.estate_sell_id)

        # Переменные для данных объекта (заполнители по умолчанию)
        complex_name = '-'
        house_name = '-'
        entrance = '-'
        number = '-'
        cat_type = '-'
        floor = '-'
        rooms = '-'
        area = '-'

        contract_num = None
        contract_date_obj = None
        contract_sum = 0

        # Если объект найден в MySQL, берем данные оттуда
        if sell:
            complex_name = sell.house.complex_name if sell.house else '-'
            house_name = sell.house.name if sell.house else '-'
            entrance = getattr(sell, 'geo_house_entrance', '-')
            number = getattr(sell, 'geo_flatnum', sell.estate_sell_category)
            cat_type = sell.estate_sell_category
            floor = sell.estate_floor
            rooms = sell.estate_rooms
            area = sell.estate_area

            # Ищем последнюю сделку
            if sell.deals:
                sorted_deals = sorted(sell.deals, key=lambda d: d.id, reverse=True)
                last_deal = sorted_deals[0]

                # 1. Номер
                num = getattr(last_deal, 'agreement_number', None)
                if not num:
                    num = getattr(last_deal, 'arles_agreement_num', None)
                contract_num = num

                # 2. Дата
                dt = last_deal.agreement_date
                if not dt:
                    dt = last_deal.preliminary_date
                contract_date_obj = dt

                # 3. Сумма
                contract_sum = last_deal.deal_sum if last_deal.deal_sum else 0

        # === ЛОГИКА "ДОПОЛНЕНИЯ" ПУСТЫХ ПОЛЕЙ РУЧНЫМИ ДАННЫМИ ===

        # Номер
        display_num = contract_num
        if not display_num or display_num == '-':
            display_num = canc.manual_number if canc.manual_number else '-'

        # Дата
        display_date = '-'
        if contract_date_obj:
            display_date = contract_date_obj.strftime('%d.%m.%Y')
        elif canc.manual_date:
            display_date = canc.manual_date.strftime('%d.%m.%Y')

        # Сумма
        display_sum = contract_sum
        if (not display_sum or display_sum == 0) and canc.manual_sum:
            display_sum = canc.manual_sum

        item = {
            'registry_id': canc.id,
            'sell_id': canc.estate_sell_id,
            'cancellation_date': canc.created_at.strftime('%d.%m.%Y'),

            'complex': complex_name,
            'house': house_name,
            'entrance': entrance,
            'number': number,
            'type': cat_type,
            'floor': floor,
            'rooms': rooms,
            'area': area,

            'contract_number': display_num,
            'contract_date': display_date,
            'contract_sum': display_sum,

            # Данные для заполнения формы в модальном окне (сырые значения)
            'manual_number_raw': canc.manual_number or '',
            'manual_date_raw': canc.manual_date.strftime('%Y-%m-%d') if canc.manual_date else '',
            'manual_sum_raw': canc.manual_sum or ''
        }
        result.append(item)

    # Сортировка по дате расторжения (от новых к старым)
    result.sort(key=lambda x: datetime.strptime(x['cancellation_date'], '%d.%m.%Y'), reverse=True)

    return result


def add_cancellation(sell_id: int):
    """Добавляет объект в реестр расторжений."""
    default_session = get_default_session()
    mysql_session = get_mysql_session()

    sell = mysql_session.query(EstateSell).get(sell_id)
    if not sell:
        return False, f"Объект {sell_id} не найден."

    exists = default_session.query(CancellationRegistry).filter_by(estate_sell_id=sell_id).first()
    if exists:
        return False, "Этот объект уже в списке расторжений."

    new_cancellation = CancellationRegistry(estate_sell_id=sell_id)
    default_session.add(new_cancellation)
    default_session.commit()
    return True, "Объект добавлен в расторжения."


def delete_cancellation(registry_id: int):
    """Удаляет запись из реестра расторжений."""
    default_session = get_default_session()
    item = default_session.query(CancellationRegistry).get(registry_id)
    if item:
        default_session.delete(item)
        default_session.commit()
        return True
    return False


def update_manual_data(registry_id: int, number: str, date_str: str, sum_val: float):
    """
    Обновляет ручные поля (manual_*) для записи реестра.
    Используется, если автоматические данные из MySQL отсутствуют.
    """
    default_session = get_default_session()
    item = default_session.query(CancellationRegistry).get(registry_id)
    if not item:
        return False, "Запись не найдена"

    try:
        # Обновляем поля. Если пришла пустая строка - ставим None
        item.manual_number = number if number else None

        if date_str:
            item.manual_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            item.manual_date = None

        item.manual_sum = sum_val if sum_val else None

        default_session.commit()
        return True, "Данные успешно обновлены"
    except Exception as e:
        default_session.rollback()
        return False, f"Ошибка сохранения: {e}"