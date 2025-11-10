# app/core/db_utils.py
from ..core.extensions import db

def get_mysql_session():
    """Возвращает сессию, привязанную к MySQL."""
    return db.session.using_bind('mysql_source')

def get_planning_session():
    """Возвращает сессию, привязанную к planning.db."""
    return db.session.using_bind('planning_db')

def get_default_session():
    """Возвращает сессию по умолчанию (main_app.db)."""
    return db.session