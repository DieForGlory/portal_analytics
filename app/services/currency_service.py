# app/services/currency_service.py

import requests
from datetime import datetime
from app.core.extensions import db
from app.models.finance_models import CurrencySettings
from flask import current_app
from ..core.db_utils import get_default_session

# API Центрального Банка Узбекистана для курса доллара
CBU_API_URL = "https://cbu.uz/ru/arkhiv-kursov-valyut/json/USD/"


def _get_settings():
    """Вспомогательная функция для получения единственной строки настроек."""
    default_session = get_default_session()
    settings = default_session.get(CurrencySettings, 1)
    if not settings:
        settings = CurrencySettings(id=1)
        default_session.add(settings)
        settings.manual_rate = 13050.0
        settings.update_effective_rate()
        default_session.commit()
    return settings


def update_cbu_rate():
    """
    Основная логика обновления курса с сайта ЦБ.
    Теперь это публичная функция (без подчеркивания в начале).
    """
    default_session = get_default_session()
    try:
        # Добавляем User-Agent, чтобы ЦБ не блокировал запрос
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(CBU_API_URL, headers=headers, timeout=10, verify=False)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("Error: CBU returned empty data list")
            return False

        rate_str = data[0]['Rate']
        rate_float = float(rate_str)

        settings = _get_settings()
        settings.cbu_rate = rate_float
        settings.cbu_last_updated = datetime.utcnow()

        # Если выбран источник ЦБ, обновляем и эффективный курс
        if settings.rate_source == 'cbu':
            settings.update_effective_rate()

        default_session.commit()
        print(f"Successfully updated CBU rate to: {rate_float}")
        return True

    except requests.RequestException as e:
        print(f"Error fetching CBU rate: {e}")
        default_session.rollback()
        return False
    except (ValueError, IndexError, KeyError) as e:
        print(f"Error parsing CBU data: {e}")
        default_session.rollback()
        return False


def set_rate_source(source: str):
    default_session = get_default_session()
    """Устанавливает источник курса ('cbu' или 'manual')."""
    if source not in ['cbu', 'manual']:
        raise ValueError("Source must be 'cbu' or 'manual'")

    settings = _get_settings()
    settings.rate_source = source
    settings.update_effective_rate()
    default_session.commit()


def set_manual_rate(rate: float):
    default_session = get_default_session()
    """Устанавливает курс вручную."""
    if rate <= 0:
        raise ValueError("Rate must be positive")

    settings = _get_settings()
    settings.manual_rate = rate

    if settings.rate_source == 'manual':
        settings.update_effective_rate()

    default_session.commit()


def get_current_effective_rate():
    """ЕДИНАЯ функция для получения актуального курса для всех расчетов."""
    settings = _get_settings()
    return settings.effective_rate