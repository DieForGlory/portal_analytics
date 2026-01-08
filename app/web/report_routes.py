# app/web/report_routes.py
import json
import os
import io
from app.services import refund_service
from app.services import quarterly_report_service, data_service
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
    project_dashboard_service,
    pricelist_service,
    presentation_service
)
from app.web.forms import UploadPlanForm, UploadManagerPlanForm
from ..models.finance_models import FinanceOperation
from ..models.estate_models import EstateHouse

report_bp = Blueprint('report', __name__, template_folder='templates')


@report_bp.route('/refund-report')
@login_required
@permission_required('view_plan_fact_report')
def refund_report():
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    month = request.args.get('month', today.month, type=int)

    # Получаем данные из сервиса
    data = refund_service.get_refund_report_data(year, month)

    # Получаем актуальный курс
    usd_rate = currency_service.get_current_effective_rate()

    return render_template(
        'reports/refund_report.html',
        title="Отчет по возвратам",
        data=data,
        selected_year=year,
        selected_month=month,
        years=[today.year - 1, today.year, today.year + 1],
        months=range(1, 13),
        usd_to_uzs_rate=usd_rate,
        datetime=datetime  # <--- ИСПРАВЛЕНИЕ: Передаем datetime в шаблон
    )

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
    group_by = request.args.get('group_by', 'complex')

    # Вызываем сервис, который возвращает 3 набора данных
    summary_by_complex, overall_summary, summary_by_house = inventory_service.get_inventory_summary_data()

    # Выбираем данные для таблицы в зависимости от группировки
    data = summary_by_house if group_by == 'house' else summary_by_complex

    # Рассчитываем общие итоги (Grand Totals) по всем типам недвижимости
    grand_totals = {
        'total_count': sum(m['units'] for m in overall_summary.values()),
        'total_area': sum(m['total_area'] for m in overall_summary.values()),
        'total_value': sum(m['total_value'] for m in overall_summary.values()),
    }

    usd_rate = currency_service.get_current_effective_rate()

    return render_template(
        'reports/inventory_summary.html',
        title="Товарные запасы (Цены дна)",
        data=data,  # Вложенный словарь {Проект: {Тип: Метрики}}
        totals=grand_totals,
        group_by=group_by,
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/financial-model')
@login_required
@permission_required('view_plan_fact_report')
def financial_model_selection():
    """Страница выбора ЖК для просмотра финансовой модели."""
    # Получаем список всех названий ЖК через существующий data_service
    complexes = data_service.get_all_complex_names()
    if not complexes:
        flash("Список проектов пуст", "warning")
        return redirect(url_for('main.index'))

    return render_template(
        'reports/financial_model_selection.html',
        complexes=complexes,
        title="Выбор проекта для финансовой модели"
    )


@report_bp.route('/financial-model/<path:complex_name>', methods=['GET', 'POST'])
@login_required
@permission_required('view_plan_fact_report')
def financial_model(complex_name):
    from app.services import financial_model_service
    from app.core.db_utils import get_planning_session
    from app.models.planning_models import ProjectFinancialTarget

    # Обработка сохранения данных
    if request.method == 'POST':
        session = get_planning_session()
        try:
            target = session.query(ProjectFinancialTarget).filter_by(complex_name=complex_name).first()
            if not target:
                target = ProjectFinancialTarget(complex_name=complex_name)
                session.add(target)

            target.total_construction_budget = float(request.form.get('total_construction_budget', 0))
            target.target_margin_percent = float(request.form.get('target_margin_percent', 0))
            target.estimated_other_costs = float(request.form.get('estimated_other_costs', 0))

            session.commit()
            flash("Финансовые цели обновлены", "success")
        except Exception as e:
            session.rollback()
            flash(f"Ошибка сохранения: {e}", "danger")
        finally:
            session.close()
        return redirect(url_for('report.financial_model', complex_name=complex_name))

    # Получение данных
    data = financial_model_service.get_financial_model_data(complex_name)

    # Если данных нет, создаем "пустышку" для отображения интерфейса
    if not data:
        data = {
            "target": ProjectFinancialTarget(
                complex_name=complex_name,
                total_construction_budget=0,
                target_margin_percent=0,
                estimated_other_costs=0
            ),
            "metrics": {
                "total_target_revenue": 0,
                "fact_revenue": 0,
                "remaining_area": 0,
                "recommended_price": 0,
                "completion_percent": 0
            },
            "monthly_flow": []
        }

    return render_template(
        'reports/financial_model.html',
        complex_name=complex_name,
        data=data
    )


@report_bp.route('/deal-registry-report')
@login_required
@permission_required('view_inventory_report')
def deal_registry_report():
    page = request.args.get('page', 1, type=int)
    # per_page можно вынести в настройки
    data, pagination = report_service.get_deal_registry_report_data(page=page, per_page=50)

    return render_template(
        'reports/deal_registry_report.html',
        title="Реестр сделок и недвижимости",
        data=data,
        pagination=pagination
    )


@report_bp.route('/export-deal-registry')
@login_required
@permission_required('view_inventory_report')
def export_deal_registry():
    excel_stream = report_service.generate_deal_registry_excel()
    if excel_stream is None:
        flash("Нет данных для экспорта.", "warning")
        return redirect(url_for('report.deal_registry_report'))

    filename = f"deal_registry_{date.today()}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@report_bp.route('/export-inventory-summary')
@login_required
@permission_required('view_inventory_report')
def export_inventory_summary():
    selected_currency = request.args.get('currency', 'UZS')
    usd_rate = currency_service.get_current_effective_rate()

    # Изменено: получаем summary_by_house
    _, _, summary_by_house = inventory_service.get_inventory_summary_data()

    # Изменено: передаем summary_by_house в генератор excel
    excel_stream = inventory_service.generate_inventory_excel(summary_by_house, selected_currency, usd_rate)

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


@report_bp.route('/reports/quarterly-analytics')
@login_required
@permission_required('view_plan_fact_report')
def quarterly_analytics():
    complexes = data_service.get_all_complex_names()
    if not complexes:
        flash("Список проектов пуст", "warning")
        return redirect(url_for('main.index'))

    complex_name = request.args.get('complex', complexes[0])
    year = request.args.get('year', datetime.now().year, type=int)
    quarter = request.args.get('quarter', (datetime.now().month - 1) // 3 + 1, type=int)

    usd_rate = currency_service.get_current_effective_rate()
    data = quarterly_report_service.get_quarterly_analytics(complex_name, year, quarter)

    return render_template(
        'reports/quarterly_analytical_report.html',
        data=data,
        complexes=complexes,
        selected_complex=complex_name,
        selected_year=year,
        selected_quarter=quarter,
        usd_to_uzs_rate=usd_rate
    )


@report_bp.route('/project-dashboard/<path:complex_name>/generate-pricelist', methods=['POST'])
@login_required
@permission_required('view_project_dashboard')
def generate_pricelist_files(complex_name):
    prop_type = request.form.get('property_type')
    percent_val = request.form.get('percent', '0').replace(',', '.')
    percent = float(percent_val) / 100
    file_format = request.form.get('format')

    # Сервис возвращает реестр (results) и статистику для шаблонов (stats)
    results, stats = pricelist_service.calculate_new_prices(complex_name, prop_type, percent)

    if results is None:  # Обработка ошибки (например, нет версии скидок)
        flash(stats, "warning")
        return redirect(url_for('report.project_dashboard', complex_name=complex_name))

    if file_format == 'excel':
        # ИСПРАВЛЕНО: Добавлен аргумент stats
        file_stream = pricelist_service.generate_pricelist_excel(results, stats)
        filename = f"Pricelist_Full_{complex_name}_{date.today()}.xlsx"
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    else:
        file_stream = presentation_service.generate_pricelist_pptx(complex_name, prop_type, percent, stats)
        filename = f"Pricelist_Analysis_{complex_name}.pptx"
        mimetype = 'application/vnd.openxmlformats-officedocument.presentationml.presentation'

    return send_file(file_stream, download_name=filename, as_attachment=True, mimetype=mimetype)

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
        # 1. Смена источника
        if 'set_source' in request.form:
            source = request.form.get('rate_source')
            try:
                currency_service.set_rate_source(source)
                flash(f"Источник курса изменен на '{source}'.", "success")
            except ValueError as e:
                flash(str(e), "danger")

        # 2. Установка ручного курса
        elif 'set_manual_rate' in request.form:
            try:
                rate = float(request.form.get('manual_rate'))
                currency_service.set_manual_rate(rate)
                flash(f"Ручной курс успешно установлен: {rate}.", "success")
            except (ValueError, TypeError):
                flash("Неверное значение для ручного курса.", "danger")

        # 3. ДОБАВЛЕНО: Принудительное обновление курса ЦБ
        elif 'update_cbu' in request.form:
            success = currency_service.update_cbu_rate()
            if success:
                flash("Курс ЦБ успешно обновлен.", "success")
            else:
                flash("Не удалось обновить курс ЦБ. Проверьте логи.", "danger")

        return redirect(url_for('report.currency_settings'))

    settings = currency_service._get_settings()
    return render_template('settings/currency_settings.html', settings=settings, title="Настройки курса валют")


@report_bp.route('/export-annual-plan-fact')
@login_required
@permission_required('view_plan_fact_report')
def export_annual_plan_fact():
    year = request.args.get('year', date.today().year, type=int)

    # Вызываем функцию генерации из сервиса
    excel_stream = report_service.generate_annual_report_excel(year)

    if excel_stream is None:
        flash("Не удалось сгенерировать годовой отчет.", "danger")
        return redirect(url_for('report.plan_fact_report'))

    filename = f"Annual_Report_{year}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@report_bp.route('/export-commercial-inventory')
@login_required
@permission_required('view_inventory_report')
def export_commercial_inventory():
    selected_currency = request.args.get('currency', 'UZS')
    usd_rate = currency_service.get_current_effective_rate()

    excel_stream = inventory_service.generate_commercial_inventory_excel(selected_currency, usd_rate)

    if excel_stream is None:
        flash("Нет данных по коммерческой недвижимости для экспорта.", "warning")
        return redirect(url_for('report.inventory_summary'))

    filename = f"commercial_inventory_{selected_currency}_{date.today()}.xlsx"
    return send_file(
        excel_stream,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
@report_bp.route('/export-plan-fact')
@login_required
@permission_required('view_plan_fact_report')
def export_plan_fact():
    today = date.today()
    year = request.args.get('year', today.year, type=int)
    month = request.args.get('month', today.month, type=int)
    period = request.args.get('period', 'monthly')
    prop_type = request.args.get('property_type', 'All')
    currency = request.args.get('currency', 'UZS')

    # Получаем текущий курс
    usd_rate = currency_service.get_current_effective_rate()

    # Вызываем обновленную функцию
    excel_stream = report_service.generate_plan_fact_excel(
        year, month, prop_type, period=period, currency=currency, rate=usd_rate
    )

    if excel_stream is None:
        flash("Нет данных для экспорта.", "warning")
        return redirect(url_for('report.plan_fact_report'))

    filename = f"plan_fact_{prop_type}_{period}_{currency}_{date.today()}.xlsx"
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
@report_bp.route('/project-passport/competitor-template')
@login_required
@permission_required('manage_settings') # Используем право админа
def download_competitor_template():
    """Отдает Excel-шаблон для таблицы конкурентов."""
    excel_stream = project_dashboard_service.generate_competitor_template_excel()
    return send_file(
        excel_stream,
        download_name='competitor_analysis_template.xlsx',
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@report_bp.route('/project-passport/upload-competitors/<path:complex_name>', methods=['POST'])
@login_required
@permission_required('manage_settings') # Используем право админа
def upload_competitor_data(complex_name):
    """Обрабатывает загрузку Excel-файла с данными о конкурентах."""
    if 'competitor_file' not in request.files:
        flash('Файл не найден в запросе.', 'danger')
        return redirect(url_for('report.project_passport', complex_name=complex_name))

    file = request.files['competitor_file']
    if file.filename == '':
        flash('Файл не выбран.', 'warning')
        return redirect(url_for('report.project_passport', complex_name=complex_name))

    if file:
        try:
            message = project_dashboard_service.process_competitor_excel(complex_name, file)
            flash(message, 'success')
        except Exception as e:
            flash(f'Ошибка при обработке файла: {e}', 'danger')

    return redirect(url_for('report.project_passport', complex_name=complex_name))


@report_bp.route('/sales-pace-report')
@login_required
@permission_required('view_plan_fact_report')
def sales_pace_report():
    """
    Отдельная страница отчета "Темп продаж" (Сравнение проектов).
    """
    # Теперь берем данные из report_service
    pace_data = report_service.get_sales_pace_comparison_data()

    charts_json = json.dumps(pace_data)

    return render_template(
        'reports/sales_pace_report.html',
        title="Сравнение темпов продаж",
        charts_json=charts_json
    )

@report_bp.route('/project-passport/download-pptx/<path:complex_name>')
@login_required
@permission_required('view_project_dashboard')  # Используем то же право
def download_project_passport_pptx(complex_name):
    """
    Генерирует и отдает "Паспорт проекта" в виде .pptx файла.
    """
    try:
        # 1. Вызываем новый сервис для генерации файла
        file_stream = project_dashboard_service.generate_passport_pptx(complex_name)

        if file_stream is None:
            flash(f"Не удалось сгенерировать презентацию для '{complex_name}'.", "danger")
            return redirect(url_for('report.project_passport', complex_name=complex_name))

        # 2. Формируем имя файла
        filename = f"Passport_{complex_name}_{date.today().isoformat()}.pptx"

        # 3. Отправляем файл пользователю
        return send_file(
            file_stream,
            download_name=filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.presentationml.presentation'
        )
    except Exception as e:
        current_app.logger.error(f"Ошибка генерации PPTX для {complex_name}: {e}")
        flash(f"Произошла внутренняя ошибка при создании файла: {e}", "danger")
        return redirect(url_for('report.project_passport', complex_name=complex_name))