from app import create_app
from app.core.db_utils import get_default_session
from sqlalchemy import text


def apply_migration():
    """Добавляет новые поля в таблицу cancellation_registry с использованием контекста приложения."""
    # 1. Создаем экземпляр приложения
    app = create_app()

    # 2. Входим в контекст приложения
    with app.app_context():
        session = get_default_session()
        engine = session.get_bind()

        alter_commands = [
            "ALTER TABLE cancellation_registry ADD COLUMN is_free BOOLEAN DEFAULT FALSE",
            "ALTER TABLE cancellation_registry ADD COLUMN is_no_money BOOLEAN DEFAULT FALSE",
            "ALTER TABLE cancellation_registry ADD COLUMN is_change_object BOOLEAN DEFAULT FALSE"
        ]

        with engine.connect() as conn:
            print("Начало миграции...")
            for cmd in alter_commands:
                try:
                    conn.execute(text(cmd))
                    conn.commit()
                    print(f"Успешно: {cmd}")
                except Exception as e:
                    print(f"Ошибка или поле уже существует: {e}")
            print("Миграция завершена.")


if __name__ == "__main__":
    apply_migration()