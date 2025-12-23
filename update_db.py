# migrate_frozen_fields.py

import sys
import os

sys.path.append(os.getcwd())

from app import create_app
from app.core.extensions import db
from sqlalchemy import text, inspect

app = create_app()


def add_column_safe(table_name, column_name, column_type):
    """Безопасное добавление колонки (из update_db.py)."""
    inspector = inspect(db.engine)
    existing_columns = [col['name'] for col in inspector.get_columns(table_name)]

    if column_name not in existing_columns:
        print(f"[*] Добавляю колонку '{column_name}' в '{table_name}'...")
        try:
            with db.engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
                conn.commit()
            print(f"    -> Успешно.")
        except Exception as e:
            print(f"    -> Ошибка: {e}")
    else:
        print(f"[v] Колонка '{column_name}' уже существует.")


if __name__ == '__main__':
    with app.app_context():
        print("--- МИГРАЦИЯ ПОЛЕЙ РЕЕСТРА РАСТОРЖЕНИЙ ---")

        fields = [
            ('complex_name', 'VARCHAR(255)'),
            ('house_name', 'VARCHAR(255)'),
            ('entrance', 'VARCHAR(50)'),
            ('number', 'VARCHAR(50)'),
            ('cat_type', 'VARCHAR(100)'),
            ('floor', 'VARCHAR(50)'),
            ('rooms', 'VARCHAR(50)'),
            ('area', 'FLOAT'),
            ('contract_number', 'VARCHAR(100)'),
            ('contract_date', 'DATE'),
            ('contract_sum', 'FLOAT')
        ]

        for name, col_type in fields:
            add_column_safe('cancellation_registry', name, col_type)

        print("--- МИГРАЦИЯ ЗАВЕРШЕНА ---")