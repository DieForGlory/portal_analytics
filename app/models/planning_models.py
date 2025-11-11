# app/models/planning_models.py

from app.core.extensions import db
from sqlalchemy import Enum as SQLAlchemyEnum, func, UniqueConstraint
from . import auth_models
import enum


class PropertyType(enum.Enum):
    FLAT = 'Квартира'
    COMM = 'Коммерческое помещение'
    GARAGE = 'Парковка'
    STORAGEROOM = 'Кладовое помещение'


class PaymentMethod(enum.Enum):
    FULL_PAYMENT = '100% оплата'
    MORTGAGE = 'Ипотека'


class DiscountVersion(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'discount_versions'
    id = db.Column(db.Integer, primary_key=True)
    version_number = db.Column(db.Integer, nullable=False, unique=True)
    comment = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())
    was_ever_activated = db.Column(db.Boolean, default=False, nullable=False)
    changes_summary_json = db.Column(db.Text, nullable=True)
    summary_sent_at = db.Column(db.DateTime(timezone=True), nullable=True)
    discounts = db.relationship('Discount', back_populates='version', cascade="all, delete-orphan")
    complex_comments = db.relationship('ComplexComment', back_populates='version', cascade="all, delete-orphan")


class SalesPlan(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'sales_plans'
    id = db.Column(db.Integer, primary_key=True)
    complex_name = db.Column(db.String(255), nullable=False, index=True)
    property_type = db.Column(db.String(100), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)
    plan_units = db.Column(db.Integer, nullable=False, default=0)
    plan_volume = db.Column(db.Float, nullable=False, default=0.0)
    plan_income = db.Column(db.Float, nullable=False, default=0.0)
    __table_args__ = (
        db.UniqueConstraint('year', 'month', 'complex_name', 'property_type', name='_plan_period_complex_prop_uc'),
    )


class Discount(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'discounts'
    id = db.Column(db.Integer, primary_key=True)
    version_id = db.Column(db.Integer, db.ForeignKey('discount_versions.id'), nullable=False, index=True)
    complex_name = db.Column(db.String(255), nullable=False, index=True)
    property_type = db.Column(SQLAlchemyEnum(PropertyType), nullable=False)
    payment_method = db.Column(SQLAlchemyEnum(PaymentMethod), nullable=False)
    mpp = db.Column(db.Float, default=0.0)
    rop = db.Column(db.Float, default=0.0)
    kd = db.Column(db.Float, default=0.0)
    opt = db.Column(db.Float, default=0.0)
    gd = db.Column(db.Float, default=0.0)
    holding = db.Column(db.Float, default=0.0)
    shareholder = db.Column(db.Float, default=0.0)
    action = db.Column(db.Float, default=0.0)
    cadastre_date = db.Column(db.Date, nullable=True)
    version = db.relationship('DiscountVersion', back_populates='discounts')
    __table_args__ = (
        db.UniqueConstraint('version_id', 'complex_name', 'property_type', 'payment_method',
                            name='_version_complex_prop_payment_uc'),
    )


class ComplexComment(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'complex_comments'
    id = db.Column(db.Integer, primary_key=True)
    version_id = db.Column(db.Integer, db.ForeignKey('discount_versions.id'), nullable=False)
    complex_name = db.Column(db.String(255), nullable=False, index=True)
    comment = db.Column(db.Text, nullable=True)
    version = db.relationship('DiscountVersion', back_populates='complex_comments')
    __table_args__ = (
        db.UniqueConstraint('version_id', 'complex_name', name='_version_complex_uc'),
    )

class ZeroMortgageMatrix(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'zero_mortgage_matrix'
    id = db.Column(db.Integer, primary_key=True)
    term_months = db.Column(db.Integer, nullable=False)
    dp_percent = db.Column(db.Integer, nullable=False)
    cashback_percent = db.Column(db.Float, nullable=False)

    __table_args__ = (
        db.UniqueConstraint('term_months', 'dp_percent', name='_term_dp_uc'),
    )
class CalculatorSettings(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'calculator_settings'
    id = db.Column(db.Integer, primary_key=True)
    standard_installment_whitelist = db.Column(db.Text, nullable=True)
    dp_installment_whitelist = db.Column(db.Text, nullable=True)
    dp_installment_max_term = db.Column(db.Integer, default=6)
    time_value_rate_annual = db.Column(db.Float, default=16.5)
    standard_installment_min_dp_percent = db.Column(db.Float, default=15.0)
    zero_mortgage_whitelist = db.Column(db.Text, nullable=True)

class ManagerSalesPlan(db.Model):
    __bind_key__ = 'planning_db'
    __tablename__ = 'manager_sales_plans'

    id = db.Column(db.Integer, primary_key=True)
    manager_id = db.Column(db.Integer, nullable=False, index=True)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)
    plan_volume = db.Column(db.Float, nullable=False, default=0.0)
    plan_income = db.Column(db.Float, nullable=False, default=0.0)
    manager = db.relationship(
        'app.models.auth_models.SalesManager',
        primaryjoin='ManagerSalesPlan.manager_id == foreign(app.models.auth_models.SalesManager.id)',
        backref='sales_plans'
    )

    __table_args__ = (
        db.UniqueConstraint('manager_id', 'year', 'month', name='_manager_plan_period_uc'),
    )

# --- НОВЫЕ ФУНКЦИИ-"ПЕРЕВОДЧИКИ" ---

def map_russian_to_mysql_key(russian_value: str) -> str:
    """
    Переводит русское название типа ('Квартира') в ключ MySQL ('flat').
    """
    mapping = {
        'Квартира': 'flat',
        'Коммерческое помещение': 'comm',
        'Парковка': 'garage',
        'Кладовое помещение': 'storageroom'
    }
    # Возвращаем ключ, если он есть в словаре, или само значение (на всякий случай)
    return mapping.get(russian_value, russian_value)

def map_mysql_key_to_russian_value(mysql_key: str) -> str:
    """
    Переводит ключ MySQL ('flat') в русское название ('Квартира').
    """
    mapping = {
        'flat': 'Квартира',
        'comm': 'Коммерческое помещение',
        'garage': 'Парковка',
        'storageroom': 'Кладовое помещение'
    }
    # Возвращаем русское значение, если оно есть, или сам ключ
    return mapping.get(mysql_key, mysql_key)