import sys
import os

# Добавляем текущую директорию, чтобы Python видел наш проект
sys.path.append(os.getcwd())

from app import create_app
from app.core.extensions import db
from sqlalchemy import text, inspect
# Импортируем модели, чтобы убедиться, что SQLAlchemy их "видит"
from app.models.auth_models import Permission, Role
from app.models.registry_models import CancellationRegistry

app = create_app()


def add_column_safe(table_name, column_name, column_type):
    """
    Безопасное добавление колонки через SQL ALTER TABLE.
    Работает и для SQLite, и для MySQL.
    """
    inspector = inspect(db.engine)
    # Получаем список существующих колонок
    existing_columns = [col['name'] for col in inspector.get_columns(table_name)]

    if column_name not in existing_columns:
        print(f"[*] Добавляю колонку '{column_name}' в таблицу '{table_name}'...")
        try:
            with db.engine.connect() as conn:
                # Используем raw SQL для изменения структуры
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))
                conn.commit()
            print(f"    -> Успешно.")
        except Exception as e:
            print(f"    -> Ошибка: {e}")
    else:
        print(f"[v] Колонка '{column_name}' уже существует в '{table_name}'.")


def add_permission_safe(name, description):
    """
    Создает право доступа в таблице permissions, если его нет.
    Возвращает объект Permission.
    """
    perm = Permission.query.filter_by(name=name).first()
    if not perm:
        print(f"[*] Создаю новое право доступа: {name}")
        perm = Permission(name=name, description=description)
        db.session.add(perm)
        db.session.commit()
    else:
        print(f"[v] Право '{name}' уже существует.")
    return perm


if __name__ == '__main__':
    with app.app_context():
        print("--- НАЧАЛО ОБНОВЛЕНИЯ БАЗЫ ДАННЫХ ---")

        # 1. ОБНОВЛЕНИЕ СТРУКТУРЫ ТАБЛИЦ (DDL)
        # Добавляем поля для ручного ввода в реестр расторжений
        add_column_safe('cancellation_registry', 'manual_number', 'VARCHAR(64)')
        add_column_safe('cancellation_registry', 'manual_date', 'DATE')
        add_column_safe('cancellation_registry', 'manual_sum', 'FLOAT')

        # 2. ОБНОВЛЕНИЕ ДАННЫХ (DML)
        # Добавляем новые пермишены
        p_cancel = add_permission_safe('manage_cancellations',
                                       'Управление реестром расторжений (добавление, ручной ввод)')
        p_registry = add_permission_safe('manage_registry', 'Управление реестрами сделок (VIP, Прогоны, К2)')

        # 3. ВЫДАЧА ПРАВ АДМИНУ
        # Автоматически выдаем эти права роли ADMIN, чтобы вы сразу могли работать
        admin_role = Role.query.filter_by(name='ADMIN').first()
        if admin_role:
            added = False
            if p_cancel not in admin_role.permissions:
                admin_role.permissions.append(p_cancel)
                print(f"    -> Право '{p_cancel.name}' выдано роли ADMIN.")
                added = True

            if p_registry not in admin_role.permissions:
                admin_role.permissions.append(p_registry)
                print(f"    -> Право '{p_registry.name}' выдано роли ADMIN.")
                added = True

            if added:
                db.session.commit()
                print("[*] Права администратора обновлены.")
            else:
                print("[v] У роли ADMIN уже есть все необходимые права.")
        else:
            print("[!] ВНИМАНИЕ: Роль 'ADMIN' не найдена. Назначьте права вручную через админку.")

        print("--- ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО ---")