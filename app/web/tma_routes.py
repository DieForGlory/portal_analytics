# app/web/tma_routes.py
from flask import Blueprint, render_template
from app.core.decorators import tma_auth_required
from app.services import data_service

# Оставляем url_prefix пустым или убираем его, если регистрируем в __init__
tma_bp = Blueprint('tma', __name__)

@tma_bp.route('/dashboard')
@tma_auth_required
def dashboard():
    complexes = data_service.get_all_complex_names()
    return render_template('tma/dashboard.html', complexes=complexes)

@tma_bp.route('/reports')
@tma_auth_required
def reports():
    return render_template('tma/reports_list.html')