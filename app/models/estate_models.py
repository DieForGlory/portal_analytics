from app.core.extensions import db


class EstateDeal(db.Model):
    __tablename__ = 'estate_deals'
    id = db.Column(db.Integer, primary_key=True)

    # ИЗМЕНЕНИЕ: Заменяем house_id и property_type на estate_sell_id
    estate_sell_id = db.Column(db.Integer, db.ForeignKey('estate_sells.id'), nullable=False)
    date_modified = db.Column(db.Date, nullable=True)
    deal_status_name = db.Column(db.String(100))
    agreement_date = db.Column(db.Date, nullable=True)
    preliminary_date = db.Column(db.Date, nullable=True)
    deal_sum = db.Column(db.Float, nullable=True)
    # ИЗМЕНЕНИЕ: Добавляем связь с EstateSell, чтобы легко получать всю информацию
    sell = db.relationship('EstateSell')
    deal_manager_id = db.Column(db.Integer, db.ForeignKey('sales_managers.id'), nullable=True, index=True)
    manager = db.relationship('SalesManager')
    __bind_key__ = 'mysql_source'


class EstateHouse(db.Model):
    __tablename__ = 'estate_houses'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    complex_name = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    geo_house = db.Column(db.String(50))

    sells = db.relationship('EstateSell', back_populates='house')
    __bind_key__ = 'mysql_source'

class EstateSell(db.Model):
    __tablename__ = 'estate_sells'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    house_id = db.Column(db.Integer, db.ForeignKey('estate_houses.id'), nullable=False)

    estate_sell_category = db.Column(db.String(100))
    estate_floor = db.Column(db.Integer)
    estate_rooms = db.Column(db.Integer)
    estate_price_m2 = db.Column(db.Float)

    estate_sell_status_name = db.Column(db.String(100), nullable=True)
    estate_price = db.Column(db.Float, nullable=True)
    # --- НОВОЕ ПОЛЕ ---
    estate_area = db.Column(db.Float, nullable=True)  # Площадь объекта
    finance_operations = db.relationship('FinanceOperation', back_populates='sell', cascade="all, delete-orphan")
    house = db.relationship('EstateHouse', back_populates='sells')
    deals = db.relationship('EstateDeal', back_populates='sell', cascade="all, delete-orphan")
    __bind_key__ = 'mysql_source'