document.addEventListener('DOMContentLoaded', function () {
    if (typeof charts_json_data === 'undefined') {
        console.error('Данные для графиков (charts_json_data) не найдены.');
        return;
    }

    const usdRate = parseFloat(document.body.dataset.usdRate) || 12650;
    const initializedCharts = {}; // Хранилище для созданных графиков

    // --- ОБЩАЯ ФУНКЦИЯ ФОРМАТИРОВАНИЯ ВАЛЮТ ---
    function formatCurrency(value, isUsd) {
        const prefix = isUsd ? '$' : '';
        const locale = isUsd ? 'en-US' : 'ru-RU';
        return prefix + new Intl.NumberFormat(locale, { maximumFractionDigits: 0 }).format(value);
    }

    // --- МОДУЛЬ ДЛЯ АНАЛИЗА СТОЯКОВ ---
    const riserAnalysisModule = {
        chartInstance: null,
        currentSort: 'total', // 'total', 'sold', 'remaining'

        init: () => {
            const chartCanvas = document.getElementById('riserAnalysisChart');
            if (!chartCanvas) return; // Графика нет на странице

            const houseFilter = document.getElementById('riserHouseFilter');
            const propTypeFilter = document.getElementById('riserPropTypeFilter');
            const sortButton = document.getElementById('riserSortToggle');

            if (!window.riserAnalysisData || !window.riserFilterOptions) {
                console.error('Данные для анализа стояков (riserAnalysisData или riserFilterOptions) не найдены.');
                document.getElementById('riserAnalysisError').textContent = window.i18n.no_riser_data || 'Ошибка загрузки данных.';
                document.getElementById('riserAnalysisError').classList.remove('d-none');
                return;
            }

            // 1. Заполнить фильтры
            riserAnalysisModule.populateFilters(houseFilter, propTypeFilter);

            // 2. Добавить слушатели
            houseFilter.addEventListener('change', riserAnalysisModule.updateChart);
            propTypeFilter.addEventListener('change', riserAnalysisModule.updateChart);
            sortButton.addEventListener('click', riserAnalysisModule.toggleSort);

            // 3. Первая отрисовка
            riserAnalysisModule.updateChart();
        },

        populateFilters: (houseFilter, propTypeFilter) => {
            houseFilter.innerHTML = `<option value="all">${window.i18n.all_houses}</option>` +
                window.riserFilterOptions.houses.map(h => `<option value="${h}">${h}</option>`).join('');

            propTypeFilter.innerHTML = `<option value="all">${window.i18n.all_prop_types}</option>` +
                window.riserFilterOptions.prop_types.map(p => `<option value="${p}">${p}</option>`).join('');
        },

        toggleSort: (e) => {
            const button = e.currentTarget;
            const buttonText = button.querySelector('span');

            if (riserAnalysisModule.currentSort === 'total') {
                riserAnalysisModule.currentSort = 'sold';
                buttonText.textContent = window.i18n.sort_sold_desc;
            } else if (riserAnalysisModule.currentSort === 'sold') {
                riserAnalysisModule.currentSort = 'remaining';
                buttonText.textContent = window.i18n.sort_remain_desc;
            } else {
                riserAnalysisModule.currentSort = 'total';
                buttonText.textContent = window.i18n.sort_total;
            }
            riserAnalysisModule.updateChart();
        },

        updateChart: () => {
            const selectedHouse = document.getElementById('riserHouseFilter').value;
            const selectedPropType = document.getElementById('riserPropTypeFilter').value;
            const errorDisplay = document.getElementById('riserAnalysisError');

            // 1. Фильтрация данных
            const filteredData = window.riserAnalysisData.filter(item => {
                const houseMatch = (selectedHouse === 'all' || item.house === selectedHouse);
                const propTypeMatch = (selectedPropType === 'all' || item.prop_type === selectedPropType);
                return houseMatch && propTypeMatch;
            });

            // 2. Сортировка данных
            filteredData.sort((a, b) => {
                if (riserAnalysisModule.currentSort === 'sold') {
                    return b.sold - a.sold;
                } else if (riserAnalysisModule.currentSort === 'remaining') {
                    return b.remaining - a.remaining;
                } else { // 'total'
                    return (b.sold + b.remaining) - (a.sold + a.remaining);
                }
            });

            // 3. Подготовка данных для графика
            const labels = filteredData.map(item => {
                let label = window.i18n.riser_label
                            .replace('%(rooms)s', item.rooms)
                            .replace('%(area)s', item.area);
                if (selectedHouse === 'all') {
                    label = `${item.house} / ${label}`;
                }
                return label;
            });
            const soldData = filteredData.map(item => item.sold);
            const remainingData = filteredData.map(item => item.remaining);

            // 4. Проверка на пустые данные
            if(filteredData.length === 0) {
                 errorDisplay.textContent = window.i18n.no_riser_data;
                 errorDisplay.classList.remove('d-none');
                 if(riserAnalysisModule.chartInstance) riserAnalysisModule.chartInstance.destroy();
                 riserAnalysisModule.chartInstance = null;
                 return;
            } else {
                errorDisplay.classList.add('d-none');
            }


            // 5. Отрисовка/Обновление
            const ctx = document.getElementById('riserAnalysisChart').getContext('2d');
            if (riserAnalysisModule.chartInstance) {
                riserAnalysisModule.chartInstance.data.labels = labels;
                riserAnalysisModule.chartInstance.data.datasets[0].data = soldData;
                riserAnalysisModule.chartInstance.data.datasets[1].data = remainingData;
                riserAnalysisModule.chartInstance.update();
            } else {
                riserAnalysisModule.chartInstance = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            { label: window.i18n.sold, data: soldData, backgroundColor: 'rgba(75, 192, 192, 0.7)' },
                            { label: window.i18n.remaining, data: remainingData, backgroundColor: 'rgba(255, 99, 132, 0.7)' }
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, ticks: { stepSize: 1 } } },
                        plugins: { legend: { position: 'bottom' }, tooltip: { mode: 'index' } }
                    }
                });
            }
        }
    };


    // --- РЕЕСТР ФУНКЦИЙ ДЛЯ СОЗДАНИЯ ГРАФИКОВ ---
    const chartInitializers = {
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
        'analysisCharts': () => { // Эта функция инициализирует все 3 графика спроса
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
        // --- ОБНОВЛЕННАЯ/ДОБАВЛЕННАЯ ФУНКЦИЯ ДЛЯ ГРАФИКА ЭТАЖЕЙ ---
        'floorRemaindersChart': () => {
            const chartData = charts_json_data.remainders_by_floor;
            const errorDisplay = document.getElementById('floorRemaindersError');
            const ctx = document.getElementById('floorRemaindersChart');
            if (!ctx) return;

            if (!chartData || !chartData.data || !chartData.data.length) {
                if(errorDisplay) {
                    errorDisplay.textContent = window.i18n.no_floor_data;
                    errorDisplay.classList.remove('d-none');
                }
                return;
            }
            if(errorDisplay) errorDisplay.classList.add('d-none');

            initializedCharts['floorRemaindersChart'] = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: chartData.labels,
                    datasets: [{
                        label: window.i18n.remaining_qty_short,
                        data: chartData.data,
                        backgroundColor: 'rgba(255, 159, 64, 0.7)',
                        borderColor: 'rgba(255, 159, 64, 1)'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                stepSize: 1,
                                callback: function(value) { if (Number.isInteger(value)) { return value; } } // Показать только целые числа
                            }
                        }
                    },
                    plugins: { legend: { display: false } }
                }
            });
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
            if (targetPaneId === '#remainders-pane') {
                // Инициализируем оба графика на этой вкладке, если они еще не созданы
                if (document.getElementById('riserAnalysisChart') && !initializedCharts['riserAnalysisChart']) {
                    riserAnalysisModule.init();
                    initializedCharts['riserAnalysisChart'] = true; // Отмечаем, что модуль инициализирован
                }
                if (document.getElementById('floorRemaindersChart') && !initializedCharts['floorRemaindersChart']) {
                    chartInitializers.floorRemaindersChart();
                }
            }
            if (targetPaneId === '#analysis-pane' && !initializedCharts['floorChart']) {
                chartInitializers.analysisCharts();
            }
            if (targetPaneId === '#pricing-pane' && !initializedCharts['priceDynamicsChart']) {
                chartInitializers.priceDynamicsChart(isUsd);
            }
        });
    });

    // 2. Первичная инициализация графика на активной по-умолчанию вкладке
    const isInitiallyUsd = document.getElementById('currencyToggle')?.checked;
    if (!initializedCharts['planFactChart']) {
        chartInitializers.planFactChart(isInitiallyUsd);
    }
    // Проверяем, не активна ли вкладка с остатками по умолчанию
    if(document.querySelector('#remainders-pane.show.active')) {
         if (document.getElementById('riserAnalysisChart') && !initializedCharts['riserAnalysisChart']) {
            riserAnalysisModule.init();
            initializedCharts['riserAnalysisChart'] = true;
        }
        if (document.getElementById('floorRemaindersChart') && !initializedCharts['floorRemaindersChart']) {
            chartInitializers.floorRemaindersChart();
        }
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