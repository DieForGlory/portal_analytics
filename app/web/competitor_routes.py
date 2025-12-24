# app/web/competitor_routes.py
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from app.services import competitor_service, data_service
from app.models.competitor_models import Competitor
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, send_file
from flask_login import login_required
from ..core.decorators import permission_required
from ..services import competitor_service, data_service
competitor_bp = Blueprint('competitor', __name__)

@competitor_bp.route('/competitors/map')
@login_required
def map_view():
    projects = data_service.get_all_complex_names() #
    competitors = Competitor.query.all()
    return render_template('competitors/map.html', projects=projects, competitors=competitors)

@competitor_bp.route('/competitors/compare/<int:comp_id>')
@login_required
def compare(comp_id):
    our_complex = request.args.get('our_project')
    data = competitor_service.get_comparison(comp_id, our_complex)
    return render_template('competitors/_comparison_card.html', data=data)

@competitor_bp.route('/competitors/import', methods=['GET', 'POST'])
@login_required
@permission_required('upload_data')
def import_data():
    if request.method == 'POST':
        file = request.files.get('file')
        if file:
            competitor_service.import_competitors(file)
            flash('Данные успешно импортированы', 'success')
            return redirect(url_for('competitor.map_view'))
    return render_template('competitors/import.html')

@competitor_bp.route('/competitors/export')
@login_required
@permission_required('view_plan_fact_report')
def export_data():
    output = competitor_service.export_competitors()
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='competitors_export.xlsx'
    )
@competitor_bp.route('/competitors/template/our')
@login_required
@permission_required('upload_data')
def download_our_template():
    output = competitor_service.get_our_projects_template()
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='template_our_projects.xlsx'
    )

@competitor_bp.route('/competitors/import/our', methods=['POST'])
@login_required
@permission_required('upload_data')
def import_our_data():
    file = request.files.get('file')
    if file:
        # Используем ту же логику импорта (из предыдущего шага),
        # так как названия колонок идентичны
        competitor_service.import_competitors(file)
        flash('Данные о наших ЖК успешно обновлены', 'success')
    return redirect(url_for('competitor.map_view'))