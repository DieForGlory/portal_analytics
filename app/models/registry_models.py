# app/models/registry_models.py

import enum
from datetime import datetime
from app.core.extensions import db


class RegistryType(enum.Enum):
    VIP = "vip"
    RUN = "run"
    GIFT = "gift"
    K2 = "k2"


class DealRegistry(db.Model):
    __tablename__ = 'deal_registries'

    id = db.Column(db.Integer, primary_key=True)
    estate_sell_id = db.Column(db.Integer, nullable=False, index=True)
    registry_type = db.Column(db.Enum(RegistryType), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)

    comment = db.Column(db.String(255), nullable=True)

    # Новые поля для К2
    k2_sum = db.Column(db.Float, nullable=True)
    crm_sum = db.Column(db.Float, nullable=True)

    def __repr__(self):
        return f"<DealRegistry {self.registry_type.value} - {self.estate_sell_id}>"


class CancellationRegistry(db.Model):
    __tablename__ = 'cancellation_registry'

    id = db.Column(db.Integer, primary_key=True)
    estate_sell_id = db.Column(db.Integer, nullable=False, unique=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.now)  # Дата расторжения

    # Можно добавить причину или комментарий
    comment = db.Column(db.String(255), nullable=True)

    def __repr__(self):
        return f"<Cancellation {self.estate_sell_id}>"