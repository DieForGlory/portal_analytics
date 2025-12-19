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
    Логика: Если agreement_number/date пустые, берем arles_agreement_num/preliminary_date.
    """
    default_session = get_default_session()
    mysql_session = get_mysql_session()

    # 1. Получаем список расторжений
    cancellations = default_session.query(CancellationRegistry).order_by(desc(CancellationRegistry.created_at)).all()

    if not cancellations:
        return []

    canc_map = {c.estate_sell_id: c for c in cancellations}
    sell_ids = list(canc_map.keys())

    # 2. Загружаем данные из MySQL
    sells = mysql_session.query(EstateSell).options(
        joinedload(EstateSell.house),
        joinedload(EstateSell.deals)
    ).filter(
        EstateSell.id.in_(sell_ids)
    ).all()

    result = []
    for sell in sells:
        cancellation = canc_map.get(sell.id)
        if not cancellation:
            continue

        # Ищем последнюю сделку
        last_deal = None
        contract_num = '-'
        contract_date = '-'
        contract_sum = 0

        if sell.deals:
            sorted_deals = sorted(sell.deals, key=lambda d: d.id, reverse=True)
            last_deal = sorted_deals[0]

            # --- ЛОГИКА ВЫБОРА ДАННЫХ ---

            # 1. Пробуем взять основной номер (ДДУ)
            num = getattr(last_deal, 'agreement_number', None)
            # Если пусто, берем из предварительного
            if not num:
                num = getattr(last_deal, 'arles_agreement_num', '-')
            contract_num = num

            # 2. Пробуем взять основную дату
            dt = last_deal.agreement_date
            # Если пусто, берем из предварительного
            if not dt:
                dt = last_deal.preliminary_date

            # Форматируем дату, если она есть
            if dt:
                contract_date = dt.strftime('%d.%m.%Y')
            else:
                contract_date = '-'

            contract_sum = last_deal.deal_sum if last_deal.deal_sum else 0
            # ---------------------------

        item = {
            'registry_id': cancellation.id,
            'sell_id': sell.id,
            'cancellation_date': cancellation.created_at.strftime('%d.%m.%Y'),

            'complex': sell.house.complex_name if sell.house else '-',
            'house': sell.house.name if sell.house else '-',
            'entrance': getattr(sell, 'geo_house_entrance', '-'),
            'number': getattr(sell, 'geo_flatnum', sell.estate_sell_category),
            'type': sell.estate_sell_category,
            'floor': sell.estate_floor,
            'rooms': sell.estate_rooms,
            'area': sell.estate_area,

            'contract_number': contract_num,
            'contract_date': contract_date,
            'contract_sum': contract_sum
        }
        result.append(item)

    result.sort(key=lambda x: datetime.strptime(x['cancellation_date'], '%d.%m.%Y'), reverse=True)

    return result


# ... (функции add_cancellation и delete_cancellation остаются без изменений) ...
def add_cancellation(sell_id: int):
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
    default_session = get_default_session()
    item = default_session.query(CancellationRegistry).get(registry_id)
    if item:
        default_session.delete(item)
        default_session.commit()
        return True
    return False