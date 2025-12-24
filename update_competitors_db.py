# update_competitors_db.py
import sys
import os
from sqlalchemy import text

# Добавляем путь к проекту
sys.path.append(os.getcwd())

from app import create_app
from app.core.extensions import db


def migrate():
    app = create_app()
    with app.app_context():
        print("[*] Запуск миграции для расширенной карточки ЖК...")

        # Список полей, которые могли быть пропущены в прошлых итерациях
        columns_to_add = [
            ("description", "TEXT"),
            ("is_internal", "BOOLEAN DEFAULT 0"),
            ("sold_count", "INTEGER DEFAULT 0"),
            ("avg_bottom_price", "FLOAT DEFAULT 0")
        ]

        with db.engine.connect() as conn:
            for col_name, col_type in columns_to_add:
                try:
                    conn.execute(text(f"ALTER TABLE competitors ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                    print(f"[+] Колонка '{col_name}' добавлена.")
                except Exception as e:
                    if "duplicate column name" in str(e).lower():
                        print(f"[!] Колонка '{col_name}' уже существует.")
                    else:
                        print(f"[-] Ошибка при добавлении '{col_name}': {e}")


if __name__ == "__main__":
    migrate()