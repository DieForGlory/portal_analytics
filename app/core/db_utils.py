# app/core/db_utils.py
from ..core.extensions import db


def get_mysql_session():
    """
    Возвращает НОВУЮ, ПРАВИЛЬНУЮ сессию, привязанную к MySQL.
    """
    # 1. Получаем "движок" (engine) для нужной базы
    engine = db.get_engine(bind_key='mysql_source')

    # 2. ИСПРАВЛЕНИЕ ЗДЕСЬ:
    #    Передаем 'bind=engine' как именованный аргумент,
    #    а НЕ как словарь {'bind': engine}
    return db.create_session(bind=engine)


def get_planning_session():
    """
    Возвращает НОВУЮ, ПРАВИЛЬНУЮ сессию, привязанную к planning.db.
    """
    # 1. Получаем "движок" (engine)
    engine = db.get_engine(bind_key='planning_db')

    # 2. ИСПРАВЛЕНИЕ ЗДЕСЬ:
    #    Передаем 'bind=engine' как именованный аргумент
    return db.create_session(bind=engine)


def get_default_session():
    """
    Возвращает сессию по умолчанию (main_app.db).
    """
    # db.session - это правильный способ получить сессию по умолчанию
    return db.session