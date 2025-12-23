# reset_competitors_db.py
import sys
import os

sys.path.append(os.getcwd())

from app import create_app
from app.core.extensions import db
from app.models.competitor_models import Competitor, CompetitorMedia

app = create_app()

with app.app_context():
    print("[*] Удаление старых таблиц...")
    # Удаляем в правильном порядке из-за Foreign Key
    CompetitorMedia.__table__.drop(db.engine, checkfirst=True)
    Competitor.__table__.drop(db.engine, checkfirst=True)

    print("[*] Создание новых таблиц...")
    db.create_all()
    print("[+] Структура базы данных обновлена.")