from datetime import datetime
from app.core.extensions import db

class ZeroMortgageMatrix(db.Model):
    __tablename__ = 'zero_mortgage_matrix'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    data = db.Column(db.JSON, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f'<ZeroMortgageMatrix {self.name}>'
class FinanceOperation(db.Model):
    __tablename__ = 'finances'

    id = db.Column(db.Integer, primary_key=True)
    estate_sell_id = db.Column(db.Integer, db.ForeignKey('estate_sells.id'), nullable=False)
    summa = db.Column(db.Float)
    status_name = db.Column(db.String(100))
    payment_type = db.Column(db.String(100), name='types_name')
    date_added = db.Column(db.Date)
    date_to = db.Column(db.Date, nullable=True)
    manager_id = db.Column(db.Integer, name='respons_manager_id')
    sell = db.relationship('EstateSell')
    __bind_key__ = 'mysql_source'

class CurrencySettings(db.Model):
    __tablename__ = 'currency_settings'
    id = db.Column(db.Integer, primary_key=True)
    # Какой источник используется: 'cbu' или 'manual'
    rate_source = db.Column(db.String(10), default='cbu', nullable=False)
    # Последний полученный курс от ЦБ
    cbu_rate = db.Column(db.Float, default=0.0)
    # Курс, установленный вручную
    manual_rate = db.Column(db.Float, default=0.0)
    # Актуальный курс, который используется во всех расчетах
    effective_rate = db.Column(db.Float, default=0.0)
    # Когда последний раз обновлялся курс ЦБ
    cbu_last_updated = db.Column(db.DateTime)

    # Метод для удобного обновления актуального курса
    def update_effective_rate(self):
        if self.rate_source == 'cbu':
            self.effective_rate = self.cbu_rate
        else:
            # ИСПРАВЛЕНИЕ: Устанавливаем ручной курс, а не курс ЦБ
            self.effective_rate = self.manual_rate

class ProjectObligation(db.Model):
    __tablename__ = 'project_obligations'
    __bind_key__ = 'planning_db'  # Сохраняем в той же базе, что и планы

    id = db.Column(db.Integer, primary_key=True)
    project_name = db.Column(db.String(255), nullable=False, index=True)
    obligation_type = db.Column(db.String(255), nullable=False) # Тип обязательства (например, "Оплата поставщику", "Налоги")
    amount = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(10), nullable=False, default='UZS')
    due_date = db.Column(db.Date, nullable=False, index=True)
    status = db.Column(db.String(50), default='Ожидает оплаты') # Статусы: "Ожидает оплаты", "Оплачено", "Просрочено"
    payment_date = db.Column(db.Date, nullable=True) # Фактическая дата оплаты
    comment = db.Column(db.Text, nullable=True)
    property_type = db.Column(db.String(100), nullable=True, index=True)
    __table_args__ = (
        db.UniqueConstraint('project_name', 'property_type', 'currency', name='_project_prop_currency_uc'),
    )
    def __repr__(self):
        # Добавим тип недвижимости в представление
        return f'<ProjectObligation {self.project_name} ({self.property_type}) - {self.amount}>'