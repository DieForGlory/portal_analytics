# app/web/cancellation_routes.py

from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_required
from app.services import cancellation_service
from app.core.decorators import permission_required
cancellation_bp = Blueprint('cancellations', __name__, template_folder='templates')


@cancellation_bp.route('/cancellations')
@login_required
@permission_required('manage_cancellations')
def index():
    data = cancellation_service.get_cancellations()
    return render_template('reports/cancellations.html', title="Реестр расторжений", data=data)


@cancellation_bp.route('/cancellations/add', methods=['POST'])
@login_required
@permission_required('manage_cancellations')
def add():
    sell_id = request.form.get('sell_id', type=int)
    if not sell_id:
        flash("Введите ID объекта", "danger")
        return redirect(url_for('cancellations.index'))

    success, msg = cancellation_service.add_cancellation(sell_id)
    if success:
        flash(msg, "success")
    else:
        flash(msg, "danger")

    return redirect(url_for('cancellations.index'))


@cancellation_bp.route('/cancellations/delete/<int:id>', methods=['POST'])
@login_required
@permission_required('manage_cancellations')
def delete(id):
    if cancellation_service.delete_cancellation(id):
        flash("Запись удалена", "success")
    else:
        flash("Ошибка удаления", "danger")
    return redirect(url_for('cancellations.index'))


@cancellation_bp.route('/cancellations/update_manual', methods=['POST'])
@login_required
@permission_required('manage_cancellations')
def update_manual():
    registry_id = request.form.get('registry_id', type=int)
    manual_number = request.form.get('manual_number')
    manual_date = request.form.get('manual_date')
    manual_sum = request.form.get('manual_sum', type=float)

    success, msg = cancellation_service.update_manual_data(registry_id, manual_number, manual_date, manual_sum)

    if success:
        flash(msg, "success")
    else:
        flash(msg, "danger")

    return redirect(url_for('cancellations.index'))