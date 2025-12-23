# app/services/competitor_service.py
import pandas as pd
import io
from app.models.competitor_models import Competitor, CompetitorMedia
from app.core.extensions import db
from app.services import project_dashboard_service


def import_competitors(file_stream):
    df = pd.read_excel(file_stream)
    # Ожидаемые колонки соответствуют полям модели
    for _, row in df.iterrows():
        comp = Competitor.query.filter_by(name=row['Наименование ЖК']).first() or Competitor()
        comp.name = row['Наименование ЖК']
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
        comp.planned_cadastre_date = pd.to_datetime(row.get('Плановая дата кадастра')).date() if row.get(
            'Плановая дата кадастра') else None
        comp.initial_cadastre_date = pd.to_datetime(row.get('Первоначальная дата кадастра')).date() if row.get(
            'Первоначальная дата кадастра') else None

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