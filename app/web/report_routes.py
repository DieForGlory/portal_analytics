# app/web/report_routes.py
import json
import os
from datetime import date, timedelta  # Убедитесь, что date импортирован
from datetime import datetime
from ..core.db_utils import get_planning_session, get_mysql_session, get_default_session
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, abort, send_file
from flask import jsonify
from flask_login import login_required
from sqlalchemy import or_, extract, func
from werkzeug.utils import secure_filename
from app.models.planning_models import PropertyType
from app.core.decorators import permission_required
from app.models import auth_models
from app.models import planning_models
from app.services import (
    report_service,
    selection_service,
    currency_service,
    inventory_service,
    manager_report_service,
    funnel_service,
    obligation_service,
    project_dashboard_service  # <-- 1. ДОБАВЛЕН ИМПОРТ НОВОГО СЕРВИСА
)
from app.web.forms import UploadPlanForm, UploadManagerPlanForm
from ..models.finance_models import FinanceOperation
from ..models.estate_models import EstateHouse

report_bp = Blueprint('report', __name__, template_folder='templates')


@report_bp.route('/manager-kpi-calculate/<int:manager_id>/<int:year>/<int:month>')
@login_required
@permission_required('view_manager_report')
def calculate_manager_kpi(manager_id, year, month):
    planning_session = get_planning_session()
    mysql_session = get_mysql_session()

    plan_entry = planning_session.query(planning_models.ManagerSalesPlan).filter_by(
        manager_id=manager_id, year=year, month=month
    ).first()
    plan_income = plan_entry.plan_income if plan_entry else 0.0

    fact_income_query = mysql_session.query(func.sum(FinanceOperation.summa)).filter(
        FinanceOperation.manager_id == manager_id,
        extract('year', FinanceOperation.date_added) == year,
        extract('month', FinanceOperation.date_added) == month,
        FinanceOperation.status_name == "Проведено",
        or_(
            FinanceOperation.payment_type != "Возврат поступлений при отмене сделки",
            FinanceOperation.payment_type.is_(None)
        )
    ).scalar()
    fact_income = fact_income_query or 0.0

    payment = manager_report_service.calculate_manager_kpi(plan_income, fact_income)

    completion_percent = (fact_income / plan_income * 100) if plan_income > 0 else 0

    result = {
        'manager_id': manager_id,
        'year': year,
        'month': month,
        'performance_percent': round(completion_percent, 2),
        'fact_amount': fact_income,
        'payment': payment
    }

    # Не забываем закрывать сессии
    planning_session.close()
    mysql_session.close()

    return jsonify({'success': True, 'data': result})


@report_bp.route('/export-expected-income-details')
@login_required
@permission_required('view_plan_fact_report')
def export_expected_income_details():
    ids_str = request.args.get('ids', '')
    excel_stream = report_service.generate_ids_excel(ids_str)

    if excel_stream is None:
        flash("Нет данных для экспорта.", "warning")
        return redirect(request.referrer or url_for('report.plan_fact_report'))

    filename = f"expected_income_details_{date.today()}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/inventory-summary')
@login_required
@permission_required('view_inventory_report')
def inventory_summary():
    summary_by_complex, overall_summary = inventory_service.get_inventory_summary_data()
    usd_rate = currency_service.get_current_effective_rate()
    return render_template(
        'reports/inventory_summary.html',
        title="Сводка по товарному запасу",
        summary=summary_by_complex,
        overall_summary=overall_summary,
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/export-inventory-summary')
@login_required
@permission_required('view_inventory_report')
def export_inventory_summary():
    selected_currency = request.args.get('currency', 'UZS')
    usd_rate = currency_service.get_current_effective_rate()
    summary_by_complex, _ = inventory_service.get_inventory_summary_data()
    excel_stream = inventory_service.generate_inventory_excel(summary_by_complex, selected_currency, usd_rate)
    if excel_stream is None:
        flash("Нет данных для экспорта.", "warning")
        return redirect(url_for('report.inventory_summary'))
    filename = f"inventory_summary_{selected_currency}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/download-plan-template')
@login_required
@permission_required('upload_data')
def download_plan_template():
    excel_stream = report_service.generate_plan_template_excel()
    return send_file(
        excel_stream,
        download_name='sales_plan_template.xlsx',
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/plan-fact', methods=['GET'])
@login_required
@permission_required('view_plan_fact_report')
def plan_fact_report():
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    period = request.args.get('period', 'monthly')
    month = request.args.get('month', today.month, type=int)
    prop_type = request.args.get('property_type', 'All')
    usd_rate = currency_service.get_current_effective_rate()

    is_period_view = period != 'monthly'
    total_refunds = 0

    if is_period_view:
        report_data, totals = report_service.generate_consolidated_report_by_period(year, period, prop_type)
        summary_data = []
        grand_totals = {}
        PERIOD_MONTHS = {'q1': range(1, 4), 'q2': range(4, 7), 'q3': range(7, 10), 'q4': range(10, 13),
                         'h1': range(1, 7), 'h2': range(7, 13)}
        for m in PERIOD_MONTHS.get(period, []):
            total_refunds += report_service.get_refund_data(year, m, prop_type)
    else:
        summary_data = report_service.get_monthly_summary_by_property_type(year, month)
        report_data, totals, total_refunds = report_service.generate_plan_fact_report(year, month, prop_type)
        grand_totals = report_service.calculate_grand_totals(year, month)

    property_types_for_template = ['All'] + [pt.value for pt in planning_models.PropertyType]

    return render_template('reports/plan_fact_report.html',
                           title="План-фактный отчет",
                           data=report_data,
                           summary_data=summary_data,
                           totals=totals,
                           grand_totals=grand_totals,
                           total_refunds=total_refunds,
                           years=[today.year - 1, today.year, today.year + 1],
                           months=range(1, 13),
                           property_types=property_types_for_template,
                           selected_year=year,
                           selected_month=month,
                           selected_period=period,
                           is_period_view=is_period_view,
                           usd_to_uzs_rate=usd_rate,
                           selected_prop_type=prop_type)


@report_bp.route('/upload-plan', methods=['GET', 'POST'])
@login_required
@permission_required('upload_data')
def upload_plan():
    form = UploadPlanForm()
    if form.validate_on_submit():
        f = form.excel_file.data
        filename = secure_filename(f.filename)
        upload_folder = os.path.join(current_app.root_path, 'uploads')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        f.save(file_path)
        try:
            year = form.year.data
            month = form.month.data
            result = report_service.process_plan_from_excel(file_path, year, month)
            flash(f"Файл успешно загружен. План на {month:02d}.{year} обновлен. {result}", "success")
        except Exception as e:
            flash(f"Произошла ошибка при обработке файла: {e}", "danger")
        return redirect(url_for('report.upload_plan'))
    return render_template('reports/upload_plan.html', title="Загрузка плана", form=form)


@report_bp.route('/commercial-offer/complex/<int:sell_id>')
@login_required
def generate_complex_kp(sell_id):
    card_data = selection_service.get_apartment_card_data(sell_id)
    if not card_data.get('apartment'):
        abort(404)
    calc_type = request.args.get('calc_type')
    details_json = request.args.get('details')
    if not all([calc_type, details_json]):
        flash("Отсутствуют данные для генерации КП.", "danger")
        return redirect(url_for('main.apartment_details', sell_id=sell_id))
    try:
        details = json.loads(details_json)
    except json.JSONDecodeError:
        abort(400, "Некорректный формат данных (JSON).")
    if 'payment_schedule' in details:
        for payment in details['payment_schedule']:
            # Убедимся, что дата в правильном формате (может прийти как YYYY-MM-DD)
            if isinstance(payment['payment_date'], str):
                payment['payment_date'] = datetime.strptime(payment['payment_date'], '%Y-%m-%d').date()
    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    usd_rate = currency_service.get_current_effective_rate()
    return render_template(
        'main/commercial_offer_complex.html',
        title=f"КП (сложный расчет) по объекту ID {sell_id}",
        data=card_data,
        calc_type=calc_type,
        details=details,
        current_date=current_date,
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/project-dashboard/<path:complex_name>')
@login_required
@permission_required('view_project_dashboard')
def project_dashboard(complex_name):
    selected_prop_type = request.args.get('property_type', None)

    # --- 2. ИЗМЕНЯЕМ ВЫЗОВ СЕРВИСА ---
    data = project_dashboard_service.get_project_dashboard_data(complex_name, selected_prop_type)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    if not data:
        abort(404)
    property_types = [pt.value for pt in planning_models.PropertyType]
    charts_json = json.dumps(data.get('charts', {}))
    usd_rate = currency_service.get_current_effective_rate()
    return render_template(
        'reports/project_dashboard.html',
        title=f"Аналитика по проекту {complex_name}",
        data=data,
        charts_json=charts_json,
        property_types=property_types,
        selected_prop_type=selected_prop_type,
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/project-passport/<path:complex_name>')
@login_required
@permission_required('view_project_dashboard')  # Используем то же право, что и для дашборда
def project_passport(complex_name):
    """Отображает страницу "Паспорт проекта"."""

    passport_full_data = project_dashboard_service.get_project_passport_data(complex_name)

    if not passport_full_data:
        flash(f"Проект с названием '{complex_name}' не найден.", "danger")
        return redirect(url_for('report.plan_fact_report'))
    usd_rate = currency_service.get_current_effective_rate()
    return render_template(
        'reports/project_passport.html',
        title=f"Паспорт проекта: {complex_name}",
        data=passport_full_data,
        static_data_json=json.dumps(passport_full_data.get('static_data', {})) ,
        usd_to_uzs_rate=usd_rate# Для JS
    )
@report_bp.route('/currency-settings', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def currency_settings():
    if request.method == 'POST':
        if 'set_source' in request.form:
            source = request.form.get('rate_source')
            currency_service.set_rate_source(source)
            flash(f"Источник курса изменен на '{source}'.", "success")
        if 'set_manual_rate' in request.form:
            try:
                rate = float(request.form.get('manual_rate'))
                currency_service.set_manual_rate(rate)
                flash(f"Ручной курс успешно установлен: {rate}.", "success")
            except (ValueError, TypeError):
                flash("Неверное значение для ручного курса.", "danger")
        return redirect(url_for('report.currency_settings'))
    settings = currency_service._get_settings()
    return render_template('settings/currency_settings.html', settings=settings, title="Настройки курса валют")


@report_bp.route('/export-plan-fact')
@login_required
@permission_required('view_plan_fact_report')
def export_plan_fact():
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    month = request.args.get('month', today.month, type=int)
    prop_type = request.args.get('property_type', 'All')
    excel_stream = report_service.generate_plan_fact_excel(year, month, prop_type)
    if excel_stream is None:
        flash("Нет данных для экспорта.", "warning")
        return redirect(url_for('report.plan_fact_report'))
    filename = f"plan_fact_report_{prop_type}_{month:02d}_{year}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/manager-performance-report', methods=['GET'])
@login_required
@permission_required('view_manager_report')
def manager_performance_report():
    # --- ИСПРАВЛЕНИЕ: Используем get_default_session() ---
    default_session = get_default_session()
    planning_session = get_planning_session()
    # ---

    search_query = request.args.get('q', '')
    show_only_with_plan = request.args.get('with_plan', 'false').lower() == 'true'

    # --- ИСПРАВЛЕНИЕ: Запрос к default_session ---
    query = default_session.query(auth_models.SalesManager)
    if search_query:
        query = query.filter(auth_models.SalesManager.full_name.ilike(f'%{search_query}%'))

    managers = query.order_by(auth_models.SalesManager.full_name).all()

    if show_only_with_plan:
        manager_ids_with_plans_query = planning_session.query(
            planning_models.ManagerSalesPlan.manager_id
        ).distinct().all()
        manager_ids_with_plans_set = {row[0] for row in manager_ids_with_plans_query}
        managers = [m for m in managers if m.id in manager_ids_with_plans_set]

    today = date.today()
    month_names = {
        1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
        7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
    }


    return render_template(
        'reports/manager_performance_overview.html',
        title="Выполнение планов менеджерами",
        managers=managers,
        search_query=search_query,
        show_only_with_plan=show_only_with_plan,
        today=today,
        month_names=month_names
    )


@report_bp.route('/download-kpi-report')
@login_required
@permission_required('download_kpi_report')
def download_kpi_report():
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    month = request.args.get('month', today.month, type=int)

    try:
        excel_stream = manager_report_service.generate_kpi_report_excel(year, month)

        if excel_stream is None:
            flash("Нет менеджеров с заполненным планом поступлений за выбранный период.", "warning")
            return redirect(url_for('report.manager_performance_report'))

        filename = f"KPI_Report_{month:02d}_{year}.xlsx"
        return send_file(
            excel_stream,
            download_name=filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except ValueError as e:
        flash(str(e), "danger")
        return redirect(url_for('report.manager_performance_report'))


@report_bp.route('/manager-performance-report/<int:manager_id>', methods=['GET'])
@login_required
@permission_required('view_manager_report')
def manager_performance_detail(manager_id):
    current_year = date.today().year
    year = request.args.get('year', current_year, type=int)

    performance_data = manager_report_service.get_manager_performance_details(manager_id, year)
    kpi_data = manager_report_service.get_manager_kpis(manager_id, year)
    complex_ranking = manager_report_service.get_manager_complex_ranking(manager_id)
    usd_rate = currency_service.get_current_effective_rate()

    month_names = {
        1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
        7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
    }

    if not performance_data:
        abort(404, "Менеджер не найден или данные отсутствуют.")

    return render_template(
        'reports/manager_performance_detail.html',
        title=f"Детализация по {performance_data['manager_name']}",
        manager_id=manager_id,
        data=performance_data,
        kpi_data=kpi_data,
        complex_ranking=complex_ranking,
        month_names=month_names,
        usd_to_uzs_rate=usd_rate,
        selected_year=year,
        years_for_nav=[current_year + 1, current_year, current_year - 1, current_year - 2]
    )


@report_bp.route('/upload-manager-plan', methods=['GET', 'POST'])
@login_required
@permission_required('upload_data')
def upload_manager_plan():
    form = UploadManagerPlanForm()
    if form.validate_on_submit():
        f = form.excel_file.data
        filename = secure_filename(f.filename)
        upload_folder = os.path.join(current_app.root_path, 'uploads')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        f.save(file_path)
        try:
            result = manager_report_service.process_manager_plans_from_excel(file_path)
            flash(f"Файл успешно загружен. {result}", "success")
        except Exception as e:
            flash(f"Произошла ошибка при обработке файла: {str(e)}", "danger")
        return redirect(url_for('report.manager_performance_report'))
    return render_template('reports/upload_manager_plan.html', title="Загрузка планов менеджеров", form=form)


@report_bp.route('/download-manager-plan-template')
@login_required
@permission_required('upload_data')
def download_manager_plan_template():
    excel_stream = manager_report_service.generate_manager_plan_template_excel()
    filename = f"manager_plans_template_{date.today().year}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/hall-of-fame/<path:complex_name>')
@login_required
@permission_required('view_manager_report')
def hall_of_fame(complex_name):
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    ranking_data = manager_report_service.get_complex_hall_of_fame(complex_name, start_date, end_date)

    usd_rate = currency_service.get_current_effective_rate()

    return render_template(
        'reports/hall_of_fame.html',
        title=f"Зал славы: {complex_name}",
        complex_name=complex_name,
        ranking_data=ranking_data,
        filters={'start_date': start_date, 'end_date': end_date},
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/funnel-leads')
@login_required
@permission_required('view_plan_fact_report')
def funnel_leads():
    """
    Отображает список заявок для конкретного узла воронки.
    """
    lead_ids_str = request.args.get('ids', '')
    node_name = request.args.get('name', 'Выбранные заявки')

    leads = funnel_service.get_leads_details_by_ids(lead_ids_str)

    return render_template(
        'reports/funnel_leads.html',
        title=f"Заявки из узла: {node_name}",
        leads=leads,
        node_name=node_name
    )


@report_bp.route('/sales-funnel')
@login_required
@permission_required('view_plan_fact_report')
def sales_funnel():
    end_date_str = request.args.get('end_date') or date.today().isoformat()
    start_date_str = request.args.get('start_date') or (date.today() - timedelta(days=30)).isoformat()
    view_mode = request.args.get('view_mode', 'tree')
    tree_data, _ = funnel_service.get_funnel_data(start_date_str, end_date_str)
    metrics_data = funnel_service.get_target_funnel_metrics(start_date_str, end_date_str)

    return render_template(
        'reports/sales_funnel.html',
        title="Анализ воронки продаж",
        tree_data=tree_data,
        metrics_data=metrics_data,
        filters={'start_date': start_date_str, 'end_date': end_date_str},
        active_view=view_mode
    )