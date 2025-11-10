# app/core/decorators.py

from functools import wraps
from flask import abort
from flask_login import current_user


def permission_required(permission_name):
    """
    Декоратор для ограничения доступа на основе прав (permissions).
    Пример: @permission_required('view_reports')
    """

    def wrapper(fn):
        @wraps(fn)
        def decorated_view(*args, **kwargs):
            if not current_user.is_authenticated:
                return abort(401)  # Или редирект на страницу входа

            # Используем наш новый метод .can()
            if not current_user.can(permission_name):
                abort(403)  # Доступ запрещен

            return fn(*args, **kwargs)

        return decorated_view

    return wrapper