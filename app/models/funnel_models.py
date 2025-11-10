# app/models/funnel_models.py
from app.core.extensions import db


class EstateBuy(db.Model):
    __tablename__ = 'estate_buys'

    id = db.Column(db.Integer, primary_key=True)
    date_added = db.Column(db.Date)  # <-- Используем это поле для фильтрации
    created_at = db.Column(db.DateTime)
    status_name = db.Column(db.String(32))
    custom_status_name = db.Column(db.String(255))
    __bind_key__ = 'mysql_source'

class EstateBuysStatusLog(db.Model):
    __tablename__ = 'estate_buys_statuses_log'
    manager_id = db.Column(db.Integer, db.ForeignKey('sales_managers.id'), nullable=True)
    id = db.Column(db.Integer, primary_key=True)
    log_date = db.Column(db.DateTime)  # <-- Используем это поле для фильтрации
    estate_buy_id = db.Column(db.Integer)
    status_to_name = db.Column(db.String(32))
    status_custom_to_name = db.Column(db.String(255))
    manager_id = db.Column('users_id', db.Integer, db.ForeignKey('sales_managers.id'), nullable=True)
    __bind_key__ = 'mysql_source'