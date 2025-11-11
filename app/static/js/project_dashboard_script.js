document.addEventListener('DOMContentLoaded', function () {
    if (typeof charts_json_data === 'undefined') {
        console.error('Данные для графиков (charts_json_data) не найдены.');
        return;
    }

    const usdRate = parseFloat(document.body.dataset.usdRate) || 13000;
    const initializedCharts = {}; // Хранилище для созданных графиков

    // --- ОБЩАЯ ФУНКЦИЯ ФОРМАТИРОВАНИЯ ВАЛЮТ ---
    function formatCurrency(value, isUsd) {
        const prefix = isUsd ? '$' : '';
        const locale = isUsd ? 'en-US' : 'ru-RU';
        return prefix + new Intl.NumberFormat(locale, { maximumFractionDigits: 0 }).format(value);
    }

    // --- РЕЕСТР ФУНКЦИЙ ДЛЯ СОЗДАНИЯ ГРАФИКОВ ---
    const chartInitializers = {

        /**
         * График: План/Факт по динамике
         */
        'planFactChart': (isUsd) => {
            const dynamics = charts_json_data.plan_fact_dynamics_yearly;
            if (!dynamics) return;
            const divisor = isUsd ? usdRate : 1;
            const ctx = document.getElementById('planFactChart');
            if (!ctx) return;
            initializedCharts['planFactChart'] = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: dynamics.labels,
                    datasets: [
                        { type: 'line', label: window.i18n.plan_contracting, data: dynamics.plan_volume.map(v => v / divisor), borderColor: 'rgba(54, 162, 235, 1)', fill: false, tension: 0.1 },
                        { type: 'bar', label: window.i18n.fact_contracting, data: dynamics.fact_volume.map(v => v / divisor), backgroundColor: 'rgba(75, 192, 192, 0.7)' },
                        { type: 'line', label: window.i18n.plan_income, data: dynamics.plan_income.map(v => v / divisor), borderColor: 'rgba(255, 99, 132, 1)', fill: false, tension: 0.1 },
                        { type: 'bar', label: window.i18n.fact_income, data: dynamics.fact_income.map(v => v / divisor), backgroundColor: 'rgba(255, 206, 86, 0.7)' }
                    ]
                },
                options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true, ticks: { callback: value => formatCurrency(value, isUsd) } } }, plugins: { tooltip: { callbacks: { label: context => `${context.dataset.label}: ${formatCurrency(context.parsed.y, isUsd)}` } } } }
            });
        },

        /**
         * Графики: Анализ спроса (Этаж, Комнаты, Площадь)
         */
        'analysisCharts': () => {
            if (!charts_json_data.sales_analysis) return;
            const analysisChartsToRender = [
                { id: 'floorChart', data: charts_json_data.sales_analysis.by_floor, label: window.i18n.units_sold },
                { id: 'roomsChart', data: charts_json_data.sales_analysis.by_rooms, label: window.i18n.units_sold },
                { id: 'areaChart', data: charts_json_data.sales_analysis.by_area, label: window.i18n.units_sold }
            ];
            analysisChartsToRender.forEach(chartInfo => {
                const ctx = document.getElementById(chartInfo.id);
                if (!ctx) return;
                if (!chartInfo.data || !chartInfo.data.data || !chartInfo.data.data.length) {
                    ctx.parentElement.innerHTML = '<div class="alert alert-secondary text-center">Нет данных для анализа.</div>'; return;
                }
                initializedCharts[chartInfo.id] = new Chart(ctx, {
                    type: 'bar',
                    data: { labels: chartInfo.data.labels, datasets: [{ label: chartInfo.label, data: chartInfo.data.data, backgroundColor: 'rgba(75, 192, 192, 0.7)' }] },
                    options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: true } }, plugins: { legend: { display: false } } }
                });
            });
        },

        /**
         * График: Динамика цен
         */
        'priceDynamicsChart': (isUsd) => {
            const chartData = charts_json_data.price_dynamics;
            const ctx = document.getElementById('priceDynamicsChart');
            if (!ctx) return;
            if (!chartData || !chartData.data || !chartData.data.length) {
                ctx.parentElement.innerHTML = '<div class="alert alert-secondary text-center">Нет данных для анализа динамики цен.</div>'; return;
            }
            const divisor = isUsd ? usdRate : 1;
            initializedCharts['priceDynamicsChart'] = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: chartData.labels,
                    datasets: [{ label: window.i18n.avg_price, data: chartData.data.map(p => p / divisor), borderColor: 'rgba(153, 102, 255, 1)', backgroundColor: 'rgba(153, 102, 255, 0.2)', fill: true, tension: 0.1 }]
                },
                options: { responsive: true, maintainAspectRatio: false, scales: { y: { beginAtZero: false, ticks: { callback: value => formatCurrency(value, isUsd) } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: context => `${context.dataset.label}: ${formatCurrency(context.parsed.y, isUsd)}` } } } }
            });
        },

        // --- УДАЛЕНА ФУНКЦИЯ 'paymentTypeChart' ---

        // --- НОВАЯ ФУНКЦИЯ: ЗАПОЛНЕНИЕ KPI ТЕМПА ПРОДАЖ ---
        'salesPaceMetrics': () => {
            const paceData = charts_json_data.sales_pace_kpi;
            const errorDiv = document.getElementById('salesPaceError');
            if (!paceData) {
                 if(errorDiv) {
                    errorDiv.textContent = window.i18n.no_pace_data || 'Нет данных для расчета темпа.';
                    errorDiv.classList.remove('d-none');
                }
                return;
            }
            if(errorDiv) errorDiv.classList.add('d-none');

            // Заполняем KPI
            const currentEl = document.getElementById('pace-current');
            const maxEl = document.getElementById('pace-max');
            const minEl = document.getElementById('pace-min');

            if(currentEl) currentEl.textContent = paceData.current.toFixed(1);
            if(maxEl) maxEl.textContent = paceData.max.toFixed(1);
            if(minEl) minEl.textContent = paceData.min.toFixed(1);

            // Рисуем квартальный график
            const ctx = document.getElementById('paceQuarterlyChart');
            if (ctx && paceData.quarterly_comparison && paceData.quarterly_comparison.data.length > 0) {
                initializedCharts['paceQuarterlyChart'] = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: paceData.quarterly_comparison.labels,
                        datasets: [{
                            label: window.i18n.quarterly_pace || 'Темп',
                            data: paceData.quarterly_comparison.data,
                            backgroundColor: 'rgba(153, 102, 255, 0.7)'
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } },
                        plugins: { legend: { display: false } }
                    }
                });
            }
        }
    };

    // --- ОСНОВНАЯ ЛОГИКА ---

    // 1. Слушатель переключения вкладок
    const tabs = document.querySelectorAll('button[data-bs-toggle="tab"]');
    tabs.forEach(tab => {
        tab.addEventListener('shown.bs.tab', function (event) {
            const targetPaneId = event.target.getAttribute('data-bs-target');
            const isUsd = document.getElementById('currencyToggle')?.checked;

            // Определяем, какой график нужно нарисовать

            // --- ВКЛАДКА "СТРУКТУРА ПРОДАЖ" (ИЗМЕНЕНО) ---
            if (targetPaneId === '#remainders-pane') {
                // (Pie chart УДАЛЕН)
                if (!initializedCharts['salesPaceMetrics']) { // Используем ключ-маркер
                    chartInitializers.salesPaceMetrics();
                    initializedCharts['salesPaceMetrics'] = true; // Отмечаем, что инициализировали
                }
            }

            // --- ВКЛАДКА "АНАЛИЗ СПРОСА" ---
            if (targetPaneId === '#analysis-pane' && !initializedCharts['floorChart']) {
                chartInitializers.analysisCharts();
            }

            // --- ВКЛАДКА "АНАЛИЗ ЦЕН" ---
            if (targetPaneId === '#pricing-pane' && !initializedCharts['priceDynamicsChart']) {
                chartInitializers.priceDynamicsChart(isUsd);
            }
        });
    });

    // 2. Первичная инициализация графика на активной по-умолчанию вкладке
    const isInitiallyUsd = document.getElementById('currencyToggle')?.checked;

    // Найти активную вкладку, которую установил report_script.js
    const activeTabPane = document.querySelector('.tab-content .tab-pane.show.active');

    if (!activeTabPane) {
        // На всякий случай, если ничего не найдено, запускаем дефолтный
        console.warn("Не найдена активная вкладка, запускаю график по умолчанию.");
        if (!initializedCharts['planFactChart']) {
            chartInitializers.planFactChart(isInitiallyUsd);
        }
    } else {
        const activeTabId = activeTabPane.id;
        console.log("Активная вкладка при загрузке:", activeTabId);

        // Запускаем инициализатор для той вкладки, которая сейчас активна
        if (activeTabId === 'dynamics-pane') {
            if (!initializedCharts['planFactChart']) {
                chartInitializers.planFactChart(isInitiallyUsd);
            }
        } else if (activeTabId === 'remainders-pane') {
            if (!initializedCharts['salesPaceMetrics']) {
                chartInitializers.salesPaceMetrics();
                initializedCharts['salesPaceMetrics'] = true;
            }
        } else if (activeTabId === 'analysis-pane') {
            if (!initializedCharts['floorChart']) {
                chartInitializers.analysisCharts();
            }
        } else if (activeTabId === 'pricing-pane') {
            if (!initializedCharts['priceDynamicsChart']) {
                chartInitializers.priceDynamicsChart(isInitiallyUsd);
            }
        }
        // Другие вкладки (deals, houses) графиков не имеют
    }


    // 3. Слушатель переключателя валют
    const currencyToggle = document.getElementById('currencyToggle');
    if (currencyToggle) {
        currencyToggle.addEventListener('change', () => {
            const isNowUsd = currencyToggle.checked;
            // Уничтожаем и перерисовываем только те графики, которые уже были созданы и зависят от валюты
            if (initializedCharts['planFactChart']) {
                initializedCharts['planFactChart'].destroy();
                chartInitializers.planFactChart(isNowUsd);
            }
            if (initializedCharts['priceDynamicsChart']) {
                initializedCharts['priceDynamicsChart'].destroy();
                chartInitializers.priceDynamicsChart(isNowUsd);
            }
        });
    }
});