# app/models/auth_models.py

from app.core.extensions import db
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

# --- ШАГ 1: ОБЪЯВЛЯЕМ МОДЕЛИ БЕЗ __bind_key__ ---

class Role(db.Model):
    # __bind_key__ удален, модель будет в основной БД
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)

    def __repr__(self):
        return f'<Role {self.name}>'

class Permission(db.Model):
    # __bind_key__ удален
    __tablename__ = 'permissions'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    description = db.Column(db.String(255))

class User(db.Model, UserMixin):
    # __bind_key__ удален
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True, nullable=False)
    full_name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    phone_number = db.Column(db.String(20), nullable=True)
    password_hash = db.Column(db.String(256))
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    def can(self, permission_name):
        if self.role:
            return any(p.name == permission_name for p in self.role.permissions)
        return False
    def __repr__(self):
        return f'<User {self.username}>'

class EmailRecipient(db.Model):
    # __bind_key__ удален
    __tablename__ = 'email_recipients'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, unique=True)

class SalesManager(db.Model):
    # __bind_key__ удален
    __tablename__ = 'sales_managers'
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(255), unique=True, nullable=False)
    post_title = db.Column(db.String(255), nullable=True)
    __bind_key__ = 'mysql_source'
    def __repr__(self):
        return f'<SalesManager {self.full_name}>'

# --- ШАГ 2: ОБЪЯВЛЯЕМ ВСПОМОГАТЕЛЬНУЮ ТАБЛИЦУ (БЕЗ info) ---
role_permissions = db.Table('role_permissions',
    db.Column('role_id', db.Integer, db.ForeignKey('roles.id'), primary_key=True),
    db.Column('permission_id', db.Integer, db.ForeignKey('permissions.id'), primary_key=True)
)

# --- ШАГ 3: ОПРЕДЕЛЯЕМ СВЯЗИ (RELATIONSHIPS) ---
User.role = db.relationship('Role', back_populates='users')
Role.users = db.relationship('User', back_populates='role', lazy='dynamic')

Role.permissions = db.relationship('Permission', secondary=role_permissions, back_populates='roles')
Permission.roles = db.relationship('Role', secondary=role_permissions, back_populates='permissions')

User.email_subscriptions = db.relationship('EmailRecipient', back_populates='user', uselist=False, cascade="all, delete-orphan")
EmailRecipient.user = db.relationship('User', back_populates='email_subscriptions')