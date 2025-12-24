# app/services/competitor_service.py
import pandas as pd
import io
from app.core.extensions import db
from app.models.competitor_models import Competitor
from app.services import project_dashboard_service, data_service
import numpy as np

def import_competitors(file_stream):
    df = pd.read_excel(file_stream)

    # 1. Удаляем строки, где название ЖК пустое
    df = df.dropna(subset=['Наименование ЖК'])

    # 2. Заменяем все типы NaN/NaT на None для корректной работы SQLAlchemy
    df = df.replace({np.nan: None, pd.NaT: None})

    for _, row in df.iterrows():
        # Теперь здесь точно будет строка или None, и ошибка уйдет
        name = str(row['Наименование ЖК']).strip()

        comp = Competitor.query.filter_by(name=name).first() or Competitor()
        comp.name = name

        # ... остальные поля ...
        comp.lat = row.get('Широта')
        comp.lng = row.get('Долгота')
        comp.property_class = row.get('Класс')
        comp.property_type = row.get('Тип')
        comp.ceiling_height = row.get('Высота потолков')
        comp.amenities = row.get('Благоустройство')
        comp.direct_competitor_name = row.get('Прямой конкурент')
        comp.indirect_competitor_name = row.get('Косвенный конкурент')
        comp.construction_stage = row.get('Стадия строительства')
        comp.units_count = row.get('Кол-во объектов')
        comp.avg_area = row.get('Средняя площадь')
        comp.avg_price_sqm = row.get('Средняя цена за квадрат')

        # Обработка дат (учитываем, что после .replace там может быть None)
        planned = row.get('Плановая дата кадастра')
        comp.planned_cadastre_date = pd.to_datetime(planned).date() if planned else None

        initial = row.get('Первоначальная дата кадастра')
        comp.initial_cadastre_date = pd.to_datetime(initial).date() if initial else None

        db.session.add(comp)

    db.session.commit()


def get_our_projects_template():
    # Список всех колонок
    columns = [
        'Наименование ЖК', 'Широта', 'Долгота', 'Класс', 'Тип',
        'Высота потолков', 'Благоустройство', 'Прямой конкурент',
        'Косвенный конкурент', 'Стадия строительства', 'Кол-во объектов',
        'Средняя площадь', 'Средняя цена за квадрат',
        'Плановая дата кадастра', 'Первоначальная дата кадастра'
    ]

    # 1. Получаем список всех имен наших ЖК из системы
    project_names = data_service.get_all_complex_names()

    data = []
    for name in project_names:
        # 2. Получаем актуальные KPI из MySQL
        our_data = project_dashboard_service.get_project_dashboard_data(name)
        kpi = our_data.get('kpi', {})

        # 3. Пытаемся найти существующую запись в SQLite (чтобы сохранить уже введенные ранее координаты/класс)
        existing = Competitor.query.filter_by(name=name).first()

        data.append({
            'Наименование ЖК': name,
            'Широта': existing.lat if existing else None,
            'Долгота': existing.lng if existing else None,
            'Класс': existing.property_class if existing else None,
            'Тип': 'Квартиры',  # Значение по умолчанию
            'Высота потолков': existing.ceiling_height if existing else None,
            'Благоустройство': existing.amenities if existing else None,
            'Прямой конкурент': '-',
            'Косвенный конкурент': '-',
            'Стадия строительства': 'В продаже',
            'Кол-во объектов': kpi.get('total_units', 0),
            'Средняя площадь': round(kpi.get('avg_area', 0), 2),
            'Средняя цена за квадрат': round(kpi.get('avg_price_sqm', 0), 0),
            'Плановая дата кадастра': None,
            'Первоначальная дата кадастра': existing.initial_cadastre_date if existing else None
        })

    df = pd.DataFrame(data, columns=columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='OurProjects')
    output.seek(0)
    return output

def import_our_projects(file_stream):
    df = pd.read_excel(file_stream)
    df = df.dropna(subset=['Наименование ЖК'])
    df = df.replace({np.nan: None, pd.NaT: None})

    for _, row in df.iterrows():
        name = str(row['Наименование ЖК']).strip()

        # Получаем данные из нашей БД (MySQL) через существующий сервис
        our_data = project_dashboard_service.get_project_dashboard_data(name)

        # Ищем или создаем запись в таблице конкурентов (чтобы отобразить на общей карте)
        comp = Competitor.query.filter_by(name=name).first() or Competitor()

        # Ручные поля из Excel
        comp.name = name
        comp.property_class = row.get('Класс')
        comp.amenities = row.get('Благоустройство')
        comp.ceiling_height = row.get('Высота потолков')
        initial_date = row.get('Первоначальная дата кадастра')
        comp.initial_cadastre_date = pd.to_datetime(initial_date).date() if initial_date else None

        # Координаты (тоже берем из Excel, так как в MySQL их может не быть)
        comp.lat = row.get('Широта')
        comp.lng = row.get('Долгота')

        # Автоматические поля из нашей БД
        if our_data and 'kpi' in our_data:
            kpi = our_data['kpi']
            comp.units_count = kpi.get('total_units', 0)
            comp.avg_price_sqm = kpi.get('avg_price_sqm', 0)
            comp.avg_area = kpi.get('avg_area', 0)
            # Тип недвижимости можно определить по структуре проекта в our_data
            comp.property_type = "Квартиры"  # Или логика определения типа

        db.session.add(comp)

    db.session.commit()
def get_comparison(comp_id, our_complex_name):
    competitor = Competitor.query.get(comp_id)
    # Используем существующий сервис для получения данных нашего ЖК
    our_data = project_dashboard_service.get_project_dashboard_data(our_complex_name)

    return {
        'competitor': competitor,
        'our_project': {
            'name': our_complex_name,
            'avg_price': our_data['kpi'].get('avg_price_sqm', 0),
            'units': our_data['kpi'].get('total_units', 0)
        }
    }


def export_competitors():
    # Явно определяем список колонок для шаблона
    columns = [
        'Наименование ЖК', 'Широта', 'Долгота', 'Класс', 'Тип',
        'Высота потолков', 'Благоустройство', 'Прямой конкурент',
        'Косвенный конкурент', 'Стадия строительства', 'Кол-во объектов',
        'Средняя площадь', 'Средняя цена за квадрат',
        'Плановая дата кадастра', 'Первоначальная дата кадастра'
    ]

    competitors = Competitor.query.all()
    data = []

    for c in competitors:
        data.append({
            'Наименование ЖК': c.name,
            'Широта': c.lat,
            'Долгота': c.lng,
            'Класс': c.property_class,
            'Тип': c.property_type,
            'Высота потолков': c.ceiling_height,
            'Благоустройство': c.amenities,
            'Прямой конкурент': c.direct_competitor_name,
            'Косвенный конкурент': c.indirect_competitor_name,
            'Стадия строительства': c.construction_stage,
            'Кол-во объектов': c.units_count,
            'Средняя площадь': c.avg_area,
            'Средняя цена за квадрат': c.avg_price_sqm,
            'Плановая дата кадастра': c.planned_cadastre_date,
            'Первоначальная дата кадастра': c.initial_cadastre_date
        })

    # Даже если data = [], DF будет содержать заголовки из списка columns
    df = pd.DataFrame(data, columns=columns)

    output = io.BytesIO()
    # Убедитесь, что в окружении установлен xlsxwriter (pip install xlsxwriter)
    # Если его нет, смените engine на 'openpyxl'
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Competitors')

    output.seek(0)
    return output