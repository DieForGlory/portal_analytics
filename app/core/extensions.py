# app/core/extensions.py
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Возможно, здесь или в app/__init__.py нужно импортировать новые модели,
# чтобы они были зарегистрированы в SQLAlchemy при db.create_all()
# from app.models import exclusion_models # Пример