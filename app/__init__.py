# app/__init__.py
import os
import json
from datetime import date, datetime
from sqlalchemy.orm import joinedload, selectinload
from flask import Flask, request, render_template, g, session, current_app
from flask_login import LoginManager
from flask_cors import CORS
from flask_babel import Babel
from .web.obligations_routes import obligations_bp
from .core.config import DevelopmentConfig
from .core.extensions import db
from .core.db_utils import get_default_session
from decimal import Decimal
from sqlalchemy.orm import joinedload
from app.core.extensions import db, migrate_default, migrate_planning, login_manager
from app.web.competitor_routes import competitor_bp
# 1. Инициализация расширений
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = "Пожалуйста, войдите в систему для доступа к этой странице."
login_manager.login_message_category = "info"
babel = Babel()


# 2. Пользовательский кодировщик для JSON
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return json.JSONEncoder.default(self, obj)


# 3. Функция для выбора языка
def select_locale():
    if 'language' in session and session['language'] in current_app.config['LANGUAGES'].keys():
        return session['language']
    return request.accept_languages.best_match(current_app.config['LANGUAGES'].keys())


def create_app(config_class=DevelopmentConfig):
    """
    Фабрика для создания и конфигурации экземпляра приложения Flask.
    """
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)

    # Конфигурация для мультиязычности
    app.config['BABEL_DEFAULT_LOCALE'] = 'ru'
    app.config['LANGUAGES'] = {'en': 'English', 'ru': 'Русский'}

    # Инициализация всех расширений
    CORS(app)
    db.init_app(app)

    # Миграции
    migrate_default.init_app(app, db, directory='migrations_default',
                             include_symbol=lambda name, table: table.info.get('bind_key') is None)

    migrate_planning.init_app(app, db, directory='migrations_planning',
                              include_symbol=lambda name, table: table.info.get('bind_key') == 'planning_db')
    login_manager.init_app(app)
    babel.init_app(app, locale_selector=select_locale)
    app.json_encoder = CustomJSONEncoder

    # Кастомный фильтр
    def fromjson_filter(value):
        return json.loads(value)

    app.jinja_env.filters['fromjson'] = fromjson_filter

    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except OSError as e:
        print(f"Ошибка при создании папки instance: {e}")

    with app.app_context():
        # Импорт моделей
        from .models import auth_models, planning_models, estate_models, finance_models, exclusion_models, \
            funnel_models, special_offer_models, registry_models

        # Регистрация Blueprints
        from .web.main_routes import main_bp
        from .web.auth_routes import auth_bp
        from .web.discount_routes import discount_bp
        from .web.report_routes import report_bp
        from .web.complex_calc_routes import complex_calc_bp
        from .web.settings_routes import settings_bp
        from .web.api_routes import api_bp
        from .web.special_offer_routes import special_offer_bp
        from .web.manager_analytics_routes import manager_analytics_bp

        # --- НОВЫЕ ИМПОРТЫ ---
        from .web.registry_routes import registry_bp  # Для реестра спец. сделок
        from .web.cancellation_routes import cancellation_bp  # Для реестра расторжений
        # ---------------------

        app.register_blueprint(report_bp, url_prefix='/reports')
        app.register_blueprint(main_bp)
        app.register_blueprint(auth_bp)
        app.register_blueprint(discount_bp)
        app.register_blueprint(complex_calc_bp)
        app.register_blueprint(settings_bp)
        app.register_blueprint(api_bp, url_prefix='/api/v1')
        app.register_blueprint(special_offer_bp, url_prefix='/specials')
        app.register_blueprint(manager_analytics_bp, url_prefix='/manager-analytics')
        app.register_blueprint(obligations_bp)
        app.register_blueprint(competitor_bp)
        # --- РЕГИСТРАЦИЯ НОВЫХ BLUEPRINTS ---
        app.register_blueprint(registry_bp)
        app.register_blueprint(cancellation_bp)

        # ------------------------------------

        # Загрузчик пользователя для Flask-Login
        @login_manager.user_loader
        def load_user(user_id):
            default_session = get_default_session()
            return default_session.query(auth_models.User).options(
                joinedload(auth_models.User.role).selectinload(auth_models.Role.permissions)
            ).get(int(user_id))

    @app.before_request
    def before_request_tasks():
        g.lang = str(select_locale())

    return app