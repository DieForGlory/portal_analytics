import time
from sqlalchemy import distinct
from ..core.db_utils import get_mysql_session
from app.models.estate_models import EstateSell, EstateHouse

mysql_session = get_mysql_session()
planning_session = get_planning_session()

def get_sells_with_house_info(page, per_page):
    """
    Получает предложения о продаже для конкретной страницы.
    Возвращает объект пагинации.
    """
    print(f"\n[DATA SERVICE] Запрос данных для страницы {page} ({per_page} записей на странице)...")
    start_time = time.time()

    try:
        mysql_session = get_mysql_session()
        # Запрос теперь проще. Мы получаем объекты EstateSell, связанные с EstateHouse.
        # Вместо .all() используем .paginate()
        pagination = mysql_session.query(EstateSell).join(EstateHouse).order_by(EstateSell.id.desc()).paginate(
            page=page,
            per_page=per_page,
            error_out=False
        )

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        print(f"[DATA SERVICE] ✔️ Запрос для страницы {page} выполнен за {duration} сек.")

        return pagination

    except Exception as e:
        print(f"[DATA SERVICE] ❌ ОШИБКА при запросе данных с пагинацией: {e}")
        return None


def get_all_complex_names():
    """Возвращает список уникальных названий ЖК из базы данных."""
    print("[DATA SERVICE] _names Запрос уникальных названий ЖК...")
    try:
        mysql_session = get_mysql_session()
        # Выполняем запрос, который выбирает только уникальные (distinct) названия
        results = mysql_session.query(distinct(EstateHouse.complex_name)).all()
        # Преобразуем результат (список кортежей) в простой список строк
        complex_names = [row[0] for row in results]
        print(f"[DATA SERVICE] 📈 Найдено уникальных ЖК: {len(complex_names)}")
        return complex_names
    except Exception as e:
        print(f"[DATA SERVICE] ❌ ОШИБКА при запросе названий ЖК: {e}")
        return []


def get_filter_options():
    """
    НОВАЯ ФУНКЦИЯ: Получает уникальные значения для фильтров этажей и комнат.
    """
    print("[DATA SERVICE] 🔎 Запрос уникальных значений для фильтров...")
    try:
        mysql_session = get_mysql_session()
        # Запрос уникальных этажей. Исключаем None и сортируем.
        floors = sorted([f[0] for f in mysql_session.query(distinct(EstateSell.estate_floor)).filter(
            EstateSell.estate_floor.isnot(None)).all()])
        # Запрос уникальных комнат. Исключаем None и сортируем.
        rooms = sorted([r[0] for r in mysql_session.query(distinct(EstateSell.estate_rooms)).filter(
            EstateSell.estate_rooms.isnot(None)).all()])

        print(f"[DATA SERVICE] ✔️ Найдено этажей: {len(floors)}, комнат: {len(rooms)}")
        return {'floors': floors, 'rooms': rooms}
    except Exception as e:
        print(f"[DATA SERVICE] ❌ ОШИБКА при запросе опций для фильтров: {e}")
        return {'floors': [], 'rooms': []}