document.addEventListener('DOMContentLoaded', function() {
    const currencyToggle = document.getElementById('currencyToggle');
    const currencyLabel = document.getElementById('currencyLabel');
    const usdRate = parseFloat(document.body.dataset.usdRate) || 13000;
    const exportLink = document.getElementById('export-link');

    const STORAGE_KEYS = {
        currency: 'planFactReport_currencyIsUSD',
        activeTab: 'planFactReport_activeTab'
    };

    function updateCurrency(isUsd) {
        document.querySelectorAll('.currency-value').forEach(el => {
            const uzsValue = parseFloat(el.dataset.uzsValue);
            if (isNaN(uzsValue)) return;

            let displayValue;
            if (isUsd) {
                if (currencyLabel) currencyLabel.textContent = 'USD';
                let usdValue = uzsValue / usdRate;
                displayValue = '$' + usdValue.toLocaleString('en-US', { maximumFractionDigits: 0 });
            } else {
                if (currencyLabel) currencyLabel.textContent = 'UZS';
                displayValue = uzsValue.toLocaleString('ru-RU', { maximumFractionDigits: 0 }).replace(/,/g, '.');
            }

            const link = el.querySelector('a');
            if (link) {
                link.textContent = displayValue;
            } else {
                el.textContent = displayValue;
            }
        });

        if (exportLink && exportLink.dataset.baseUrl) {
            const baseUrl = exportLink.dataset.baseUrl;
            exportLink.href = isUsd ? `${baseUrl}?currency=USD` : baseUrl;
        }
    }

    function restoreState() {
        const savedCurrencyIsUSD = localStorage.getItem(STORAGE_KEYS.currency);
        if (savedCurrencyIsUSD === 'true' && currencyToggle) {
            currencyToggle.checked = true;
        }
        updateCurrency(currencyToggle ? currencyToggle.checked : false);

        const savedTabId = localStorage.getItem(STORAGE_KEYS.activeTab);
        if (savedTabId) {
            const tabTrigger = document.querySelector(`button[data-bs-target="${savedTabId}"]`);
            if (tabTrigger) {
                const tab = new bootstrap.Tab(tabTrigger);
                tab.show();
            }
        }
    }

    function sortTable(table, column, asc = true) {
        const dirModifier = asc ? 1 : -1;
        const tBody = table.tBodies[0];
        const rows = Array.from(tBody.querySelectorAll("tr"));
        const headerCell = table.querySelector(`th:nth-child(${column + 1})`);
        if (!headerCell) return;

        const isNumeric = headerCell.dataset.type === 'numeric';

        const sortedRows = rows.sort((a, b) => {
            const aColText = a.querySelector(`td:nth-child(${column + 1})`).textContent.trim();
            const bColText = b.querySelector(`td:nth-child(${column + 1})`).textContent.trim();
            if (isNumeric) {
                const aVal = parseFloat(aColText.replace(/[^0-9.-]+/g, ""));
                const bVal = parseFloat(bColText.replace(/[^0-9.-]+/g, ""));
                return (aVal - bVal) * dirModifier;
            }
            return aColText.localeCompare(bColText, 'ru', { sensitivity: 'base' }) * dirModifier;
        });

        tBody.append(...sortedRows);
        table.querySelectorAll("th").forEach(th => th.classList.remove("th-asc", "th-desc"));

        headerCell.classList.toggle("th-asc", asc);
        headerCell.classList.toggle("th-desc", !asc);
    }

    document.querySelectorAll("#summaryTable th[data-sortable]").forEach(headerCell => {
        headerCell.addEventListener("click", () => {
            const tableElement = headerCell.closest('table');
            const headerIndex = Array.prototype.indexOf.call(headerCell.parentElement.children, headerCell);
            const currentIsAsc = headerCell.classList.contains("th-asc");
            sortTable(tableElement, headerIndex, !currentIsAsc);
        });
    });

    const searchInput = document.getElementById('projectSearchInput');
    const tableBody = document.getElementById('summaryTableBody');
    if (searchInput && tableBody) {
        searchInput.addEventListener('input', function() {
            const searchTerm = searchInput.value.toLowerCase().trim();
            const allRows = tableBody.querySelectorAll('tr');

            allRows.forEach(row => {
                const projectNameEl = row.querySelector('td:first-child');
                if (projectNameEl) {
                    const projectName = projectNameEl.textContent.toLowerCase();
                    row.style.display = projectName.includes(searchTerm) ? '' : 'none';
                }
            });
        });
    }

    if (currencyToggle) {
        currencyToggle.addEventListener('change', function() {
            localStorage.setItem(STORAGE_KEYS.currency, this.checked);
            updateCurrency(this.checked);
        });
    }

    document.querySelectorAll('button[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function (event) {
            const activeTabId = event.target.dataset.bsTarget;
            localStorage.setItem(STORAGE_KEYS.activeTab, activeTabId);
        });
    });

    // Инициализация при загрузке
    if (exportLink) {
        exportLink.dataset.baseUrl = exportLink.href;
    }
    restoreState();
});