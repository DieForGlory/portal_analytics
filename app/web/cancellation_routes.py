# app/web/cancellation_routes.py

from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_required
from app.services import cancellation_service

cancellation_bp = Blueprint('cancellations', __name__, template_folder='templates')


@cancellation_bp.route('/cancellations')
@login_required
def index():
    data = cancellation_service.get_cancellations()
    return render_template('reports/cancellations.html', title="Реестр расторжений", data=data)


@cancellation_bp.route('/cancellations/add', methods=['POST'])
@login_required
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
def delete(id):
    if cancellation_service.delete_cancellation(id):
        flash("Запись удалена", "success")
    else:
        flash("Ошибка удаления", "danger")
    return redirect(url_for('cancellations.index'))