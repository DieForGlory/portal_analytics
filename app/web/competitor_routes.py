# app/web/competitor_routes.py
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from app.services import competitor_service, data_service
from app.models.competitor_models import Competitor

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