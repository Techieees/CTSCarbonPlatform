import {
  formatFull,
  getAxisLabel,
  getGrid,
  getLegend,
  getTooltipBase,
  initChart,
  makeGradient
} from "./charts/echarts_theme.js";

const API_URL = "/api/audit-2025";
const ROW_TOTAL_COLUMN = "Row Total (t)";
const SHARE_TOTAL_COLUMN = "Company Share in Total (%)";
const SHARE_MONTH_COLUMN = "Company Share in Month (%)";
const BASE_COLUMNS = ["Month", "Company"];
const META_COLUMNS = [ROW_TOTAL_COLUMN, SHARE_TOTAL_COLUMN, SHARE_MONTH_COLUMN];

const summaryKeys = {
  totalEmissions: document.querySelector("[data-summary-key='totalEmissions']"),
  companyCount: document.querySelector("[data-summary-key='companyCount']"),
  monthCount: document.querySelector("[data-summary-key='monthCount']"),
  highestCompany: document.querySelector("[data-summary-key='highestCompany']"),
  highestCompanyValue: document.querySelector("[data-summary-key='highestCompanyValue']"),
  highestCategory: document.querySelector("[data-summary-key='highestCategory']"),
  highestCategoryValue: document.querySelector("[data-summary-key='highestCategoryValue']")
};

const elements = {
  error: document.getElementById("audit2025Error"),
  year: document.getElementById("audit2025YearFilter"),
  month: document.getElementById("audit2025MonthFilter"),
  company: document.getElementById("audit2025CompanyFilter"),
  category: document.getElementById("audit2025CategoryFilter"),
  analysisCategory: document.getElementById("audit2025AnalysisCategory"),
  reset: document.getElementById("audit2025ResetFilters"),
  search: document.getElementById("audit2025Search"),
  exportCsv: document.getElementById("audit2025ExportCsv"),
  tableMeta: document.getElementById("audit2025TableMeta"),
  tableHead: document.getElementById("audit2025TableHead"),
  tableBody: document.getElementById("audit2025TableBody"),
  categoryTotalsBody: document.getElementById("audit2025CategoryTotalsBody"),
  trendChart: document.getElementById("audit2025TrendChart"),
  companyChart: document.getElementById("audit2025CompanyChart"),
  categoryStackChart: document.getElementById("audit2025CategoryStackChart"),
  categoryDetailChart: document.getElementById("audit2025CategoryDetailChart"),
  shareTotalChart: document.getElementById("audit2025ShareTotalChart"),
  shareMonthChart: document.getElementById("audit2025ShareMonthChart")
};

const state = {
  payload: null,
  filters: {
    year: "all",
    month: "all",
    company: "all",
    category: "all"
  },
  analysisCategory: null,
  search: "",
  sort: {
    column: "_audit_month_sort",
    direction: "asc"
  },
  displayRows: [],
  displayColumns: []
};

const numberFormatter = new Intl.NumberFormat("en", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const integerFormatter = new Intl.NumberFormat("en", { maximumFractionDigits: 0 });

function asNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function sumValues(values) {
  return values.reduce((total, value) => total + (asNumber(value) ?? 0), 0);
}

function averageValues(values) {
  const numericValues = values.map(asNumber).filter((value) => value !== null);
  if (!numericValues.length) {
    return null;
  }
  return sumValues(numericValues) / numericValues.length;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showError(message) {
  if (!elements.error) {
    return;
  }
  elements.error.textContent = message;
  elements.error.classList.remove("d-none");
}

function clearError() {
  if (!elements.error) {
    return;
  }
  elements.error.textContent = "";
  elements.error.classList.add("d-none");
}

function formatValue(column, value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }
  const numericValue = asNumber(value);
  if (numericValue === null) {
    return String(value);
  }
  if (column === SHARE_TOTAL_COLUMN || column === SHARE_MONTH_COLUMN) {
    return `${numberFormatter.format(numericValue)}%`;
  }
  return numberFormatter.format(numericValue);
}

function setSummaryValue(key, value) {
  if (summaryKeys[key]) {
    summaryKeys[key].textContent = value;
  }
}

function getCategoryColumns() {
  return state.payload?.category_columns || [];
}

function getActiveCategoryColumns() {
  const selectedCategory = state.filters.category;
  if (selectedCategory && selectedCategory !== "all") {
    return [selectedCategory];
  }
  return getCategoryColumns();
}

function getVisibleColumns() {
  return [
    ...BASE_COLUMNS,
    ...getActiveCategoryColumns(),
    ...META_COLUMNS
  ];
}

function populateSelect(select, options, allLabel) {
  if (!select) {
    return;
  }
  const currentValue = select.value || "all";
  select.innerHTML = `<option value="all">${allLabel}</option>`;
  options.forEach((option) => {
    const node = document.createElement("option");
    node.value = String(option);
    node.textContent = String(option);
    select.appendChild(node);
  });
  select.value = options.includes(currentValue) ? currentValue : "all";
}

function syncAnalysisCategoryControl() {
  const categories = getCategoryColumns();
  if (!elements.analysisCategory) {
    return;
  }

  const forcedCategory = state.filters.category !== "all" ? state.filters.category : null;
  const selected = forcedCategory || state.analysisCategory || categories[0] || "";

  elements.analysisCategory.innerHTML = "";
  categories.forEach((category) => {
    const option = document.createElement("option");
    option.value = category;
    option.textContent = category;
    elements.analysisCategory.appendChild(option);
  });

  elements.analysisCategory.disabled = Boolean(forcedCategory);
  elements.analysisCategory.value = categories.includes(selected) ? selected : categories[0] || "";
  state.analysisCategory = elements.analysisCategory.value || null;
}

function populateFilters(payload) {
  populateSelect(elements.year, payload.years.map(String), "All years");
  populateSelect(elements.month, payload.months, "All months");
  populateSelect(elements.company, payload.companies, "All companies");
  populateSelect(elements.category, payload.category_columns, "All categories");
  syncAnalysisCategoryControl();
}

function getFilteredRows() {
  const records = Array.isArray(state.payload?.records) ? state.payload.records : [];
  return records.filter((record) => {
    if (state.filters.year !== "all" && String(record._audit_year || "") !== state.filters.year) {
      return false;
    }
    if (state.filters.month !== "all" && String(record._audit_month_label || "") !== state.filters.month) {
      return false;
    }
    if (state.filters.company !== "all" && String(record.Company || "") !== state.filters.company) {
      return false;
    }
    return true;
  });
}

function getSearchedRows(rows, columns) {
  const term = state.search.trim().toLowerCase();
  if (!term) {
    return rows.slice();
  }
  return rows.filter((record) =>
    columns.some((column) => {
      const rawValue = column === "Month" ? (record._audit_month_label || record.Month) : record[column];
      return formatValue(column, rawValue).toLowerCase().includes(term);
    })
  );
}

function compareRecords(a, b, column, direction) {
  const multiplier = direction === "desc" ? -1 : 1;
  const aValue = column === "_audit_month_sort" ? a._audit_month_sort : a[column];
  const bValue = column === "_audit_month_sort" ? b._audit_month_sort : b[column];
  const aNumeric = asNumber(aValue);
  const bNumeric = asNumber(bValue);
  if (aNumeric !== null && bNumeric !== null) {
    return (aNumeric - bNumeric) * multiplier;
  }
  return String(aValue ?? "").localeCompare(String(bValue ?? ""), undefined, { numeric: true, sensitivity: "base" }) * multiplier;
}

function getSortedRows(rows) {
  const column = state.sort.column || "_audit_month_sort";
  const direction = state.sort.direction || "asc";
  return rows.slice().sort((a, b) => {
    const primary = compareRecords(a, b, column, direction);
    if (primary !== 0) {
      return primary;
    }
    const secondary = compareRecords(a, b, "_audit_month_sort", "asc");
    if (secondary !== 0) {
      return secondary;
    }
    return compareRecords(a, b, "Company", "asc");
  });
}

function getMonthSeries(rows) {
  return [...new Map(
    rows
      .filter((record) => String(record._audit_month_label || "").trim())
      .sort((a, b) => String(a._audit_month_sort || "").localeCompare(String(b._audit_month_sort || "")))
      .map((record) => [String(record._audit_month_label), String(record._audit_month_sort || "")])
  ).entries()].map(([label]) => label);
}

function groupSum(rows, groupKey, valueColumn) {
  const groups = new Map();
  rows.forEach((record) => {
    const key = String(record[groupKey] || "").trim();
    if (!key) {
      return;
    }
    groups.set(key, (groups.get(key) || 0) + (asNumber(record[valueColumn]) ?? 0));
  });
  return groups;
}

function groupAverage(rows, groupKey, valueColumn) {
  const buckets = new Map();
  rows.forEach((record) => {
    const key = String(record[groupKey] || "").trim();
    const value = asNumber(record[valueColumn]);
    if (!key || value === null) {
      return;
    }
    const bucket = buckets.get(key) || [];
    bucket.push(value);
    buckets.set(key, bucket);
  });
  return new Map([...buckets.entries()].map(([key, values]) => [key, averageValues(values) ?? 0]));
}

function setEmptyChart(container, message) {
  const chart = initChart(container);
  if (!chart) {
    return;
  }
  chart.setOption({
    title: {
      text: message,
      left: "center",
      top: "middle",
      textStyle: {
        color: "#94a3b8",
        fontSize: 14,
        fontWeight: 600
      }
    },
    xAxis: { show: false },
    yAxis: { show: false },
    series: []
  }, true);
}

function renderSummary(rows) {
  const companyTotals = groupSum(rows, "Company", ROW_TOTAL_COLUMN);
  const categoryTotals = new Map(
    getCategoryColumns().map((column) => [column, sumValues(rows.map((record) => record[column]))])
  );
  const highestCompany = [...companyTotals.entries()].sort((a, b) => b[1] - a[1])[0];
  const highestCategory = [...categoryTotals.entries()].sort((a, b) => b[1] - a[1])[0];

  setSummaryValue("totalEmissions", formatValue(ROW_TOTAL_COLUMN, sumValues(rows.map((record) => record[ROW_TOTAL_COLUMN]))));
  setSummaryValue("companyCount", integerFormatter.format(companyTotals.size));
  setSummaryValue("monthCount", integerFormatter.format(new Set(rows.map((record) => record._audit_month_label).filter(Boolean)).size));
  setSummaryValue("highestCompany", highestCompany ? highestCompany[0] : "-");
  setSummaryValue("highestCompanyValue", highestCompany ? `${formatValue(ROW_TOTAL_COLUMN, highestCompany[1])} tCO2e` : "-");
  setSummaryValue("highestCategory", highestCategory ? highestCategory[0] : "-");
  setSummaryValue("highestCategoryValue", highestCategory ? `${formatValue(ROW_TOTAL_COLUMN, highestCategory[1])} tCO2e` : "-");
}

function renderTrendChart(rows) {
  if (!rows.length) {
    setEmptyChart(elements.trendChart, "No audit records for the current filter.");
    return;
  }

  const months = getMonthSeries(rows);
  const companies = [...groupSum(rows, "Company", ROW_TOTAL_COLUMN).keys()];
  const series = companies.map((company) => ({
    name: company,
    type: "line",
    smooth: true,
    symbolSize: 8,
    data: months.map((month) =>
      sumValues(
        rows
          .filter((record) => record.Company === company && record._audit_month_label === month)
          .map((record) => record[ROW_TOTAL_COLUMN])
      )
    )
  }));

  const chart = initChart(elements.trendChart);
  if (!chart) {
    return;
  }
  chart.setOption({
    tooltip: getTooltipBase((params) => {
      const items = params
        .map((item) => `${item.marker}${escapeHtml(item.seriesName)}: ${formatValue(ROW_TOTAL_COLUMN, item.value)} t`)
        .join("<br>");
      return `<strong>${escapeHtml(params[0]?.axisValue || "")}</strong><br>${items}`;
    }),
    legend: getLegend(companies.length > 1),
    grid: getGrid(),
    xAxis: {
      type: "category",
      data: months,
      axisLabel: getAxisLabel((value) => value)
    },
    yAxis: {
      type: "value",
      axisLabel: getAxisLabel((value) => formatFull(value))
    },
    series
  }, true);
}

function renderCompanyChart(rows) {
  if (!rows.length) {
    setEmptyChart(elements.companyChart, "No company comparison available.");
    return;
  }

  const selectedMetric = state.filters.category === "all" ? ROW_TOTAL_COLUMN : state.filters.category;
  const totals = [...groupSum(rows, "Company", selectedMetric).entries()].sort((a, b) => b[1] - a[1]);
  const chart = initChart(elements.companyChart);
  if (!chart) {
    return;
  }
  chart.setOption({
    tooltip: getTooltipBase((params) => {
      const item = Array.isArray(params) ? params[0] : params;
      return `${escapeHtml(item.name)}<br>${formatValue(selectedMetric, item.value)}${selectedMetric === ROW_TOTAL_COLUMN ? " t" : " tCO2e"}`;
    }),
    grid: getGrid(true),
    xAxis: {
      type: "value",
      axisLabel: getAxisLabel((value) => formatFull(value))
    },
    yAxis: {
      type: "category",
      data: totals.map(([company]) => company),
      axisLabel: getAxisLabel((value) => value)
    },
    series: [
      {
        type: "bar",
        data: totals.map(([, value]) => value),
        barMaxWidth: 22,
        itemStyle: {
          borderRadius: [0, 999, 999, 0],
          color: makeGradient(0, true)
        }
      }
    ]
  }, true);
}

function renderCategoryStackChart(rows) {
  const categories = getActiveCategoryColumns();
  if (!rows.length || !categories.length) {
    setEmptyChart(elements.categoryStackChart, "No category breakdown available.");
    return;
  }

  const months = getMonthSeries(rows);
  const chart = initChart(elements.categoryStackChart);
  if (!chart) {
    return;
  }
  chart.setOption({
    tooltip: getTooltipBase((params) => {
      const items = params
        .map((item) => `${item.marker}${escapeHtml(item.seriesName)}: ${formatValue(item.seriesName, item.value)} tCO2e`)
        .join("<br>");
      return `<strong>${escapeHtml(params[0]?.axisValue || "")}</strong><br>${items}`;
    }),
    legend: getLegend(categories.length > 1),
    grid: getGrid(),
    xAxis: {
      type: "category",
      data: months,
      axisLabel: getAxisLabel((value) => value)
    },
    yAxis: {
      type: "value",
      axisLabel: getAxisLabel((value) => formatFull(value))
    },
    series: categories.map((category, index) => ({
      name: category,
      type: "bar",
      stack: "audit-categories",
      emphasis: { focus: "series" },
      data: months.map((month) =>
        sumValues(
          rows
            .filter((record) => record._audit_month_label === month)
            .map((record) => record[category])
        )
      ),
      itemStyle: {
        borderRadius: [8, 8, 0, 0],
        color: makeGradient(index)
      }
    }))
  }, true);
}

function renderCategoryDetailChart(rows) {
  const category = state.analysisCategory;
  if (!rows.length || !category) {
    setEmptyChart(elements.categoryDetailChart, "Select a category to inspect.");
    return;
  }
  const months = getMonthSeries(rows);
  const companies = [...groupSum(rows, "Company", category).keys()];
  const chart = initChart(elements.categoryDetailChart);
  if (!chart) {
    return;
  }
  chart.setOption({
    tooltip: getTooltipBase((params) => {
      const items = params
        .map((item) => `${item.marker}${escapeHtml(item.seriesName)}: ${formatValue(category, item.value)} tCO2e`)
        .join("<br>");
      return `<strong>${escapeHtml(params[0]?.axisValue || "")}</strong><br>${items}`;
    }),
    legend: getLegend(companies.length > 1),
    grid: getGrid(),
    xAxis: {
      type: "category",
      data: months,
      axisLabel: getAxisLabel((value) => value)
    },
    yAxis: {
      type: "value",
      axisLabel: getAxisLabel((value) => formatFull(value))
    },
    series: companies.map((company) => ({
      name: company,
      type: "line",
      smooth: true,
      data: months.map((month) =>
        sumValues(
          rows
            .filter((record) => record.Company === company && record._audit_month_label === month)
            .map((record) => record[category])
        )
      )
    }))
  }, true);
}

function renderShareChart(container, rows, column, title) {
  if (!rows.length) {
    setEmptyChart(container, `No ${title.toLowerCase()} data available.`);
    return;
  }
  const averages = [...groupAverage(rows, "Company", column).entries()].sort((a, b) => b[1] - a[1]);
  const chart = initChart(container);
  if (!chart) {
    return;
  }
  chart.setOption({
    tooltip: getTooltipBase((params) => {
      const item = Array.isArray(params) ? params[0] : params;
      return `${escapeHtml(item.name)}<br>${formatValue(column, item.value)}`;
    }),
    grid: getGrid(true),
    xAxis: {
      type: "value",
      axisLabel: getAxisLabel((value) => `${formatFull(value)}%`)
    },
    yAxis: {
      type: "category",
      data: averages.map(([company]) => company),
      axisLabel: getAxisLabel((value) => value)
    },
    series: [
      {
        type: "bar",
        data: averages.map(([, value]) => value),
        barMaxWidth: 22,
        itemStyle: {
          borderRadius: [0, 999, 999, 0],
          color: makeGradient(column === SHARE_TOTAL_COLUMN ? 2 : 3, true)
        }
      }
    ]
  }, true);
}

function renderCategoryTotals(rows) {
  if (!rows.length) {
    elements.categoryTotalsBody.innerHTML = `<tr><td colspan="4" class="audit-2025-empty">No category totals available.</td></tr>`;
    return;
  }
  const totalEmissions = sumValues(rows.map((record) => record[ROW_TOTAL_COLUMN]));
  const categories = getActiveCategoryColumns();
  const totals = categories
    .map((category) => ({
      category,
      total: sumValues(rows.map((record) => record[category]))
    }))
    .sort((a, b) => b.total - a.total);

  if (!totals.length) {
    elements.categoryTotalsBody.innerHTML = `<tr><td colspan="4" class="audit-2025-empty">No category totals available.</td></tr>`;
    return;
  }

  elements.categoryTotalsBody.innerHTML = totals.map((item, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${escapeHtml(item.category)}</td>
      <td>${escapeHtml(formatValue(ROW_TOTAL_COLUMN, item.total))}</td>
      <td>${totalEmissions > 0 ? escapeHtml(formatValue(SHARE_TOTAL_COLUMN, (item.total / totalEmissions) * 100)) : ""}</td>
    </tr>
  `).join("");
}

function renderTable(rows) {
  const columns = getVisibleColumns();
  const searchedRows = getSearchedRows(rows, columns);
  const sortedRows = getSortedRows(searchedRows);
  state.displayRows = sortedRows;
  state.displayColumns = columns;

  elements.tableMeta.textContent = `${integerFormatter.format(sortedRows.length)} row(s) visible`;
  elements.tableHead.innerHTML = `
    <tr>
      ${columns.map((column) => {
        const isSorted = state.sort.column === column;
        const indicator = isSorted ? (state.sort.direction === "asc" ? "▲" : "▼") : "↕";
        return `
          <th scope="col">
            <button type="button" data-sort-column="${escapeHtml(column)}">
              <span>${escapeHtml(column)}</span>
              <span class="audit-2025-sort-indicator">${indicator}</span>
            </button>
          </th>
        `;
      }).join("")}
    </tr>
  `;

  if (!sortedRows.length) {
    elements.tableBody.innerHTML = `<tr><td colspan="${columns.length}" class="audit-2025-empty">No audit rows match the current filters.</td></tr>`;
    return;
  }

  elements.tableBody.innerHTML = sortedRows.map((record) => `
    <tr>
      ${columns.map((column) => `<td>${escapeHtml(formatValue(column, column === "Month" ? (record._audit_month_label || record.Month) : record[column]))}</td>`).join("")}
    </tr>
  `).join("");
}

function renderAll() {
  const rows = getFilteredRows();
  syncAnalysisCategoryControl();
  renderSummary(rows);
  renderTrendChart(rows);
  renderCompanyChart(rows);
  renderCategoryStackChart(rows);
  renderCategoryDetailChart(rows);
  renderShareChart(elements.shareTotalChart, rows, SHARE_TOTAL_COLUMN, "Company Share in Total");
  renderShareChart(elements.shareMonthChart, rows, SHARE_MONTH_COLUMN, "Company Share in Month");
  renderCategoryTotals(rows);
  renderTable(rows);
}

function exportCurrentTable() {
  if (!state.displayRows.length || !state.displayColumns.length) {
    return;
  }
  const csvLines = [
    state.displayColumns.map((column) => `"${String(column).replaceAll('"', '""')}"`).join(",")
  ];
  state.displayRows.forEach((record) => {
    const line = state.displayColumns.map((column) => {
      const value = column === "Month" ? (record._audit_month_label || record.Month) : record[column];
      return `"${String(formatValue(column, value)).replaceAll('"', '""')}"`;
    }).join(",");
    csvLines.push(line);
  });

  const blob = new Blob([csvLines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "audit-2025-filtered.csv";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

async function loadAuditData() {
  try {
    const response = await fetch(API_URL, {
      headers: { Accept: "application/json" },
      credentials: "same-origin"
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "Audit 2025 data could not be loaded.");
    }
    state.payload = payload;
    clearError();
    populateFilters(payload);
    renderAll();
  } catch (error) {
    state.payload = null;
    showError(error.message || "Audit 2025 data could not be loaded.");
    renderSummary([]);
    renderTable([]);
    renderCategoryTotals([]);
    setEmptyChart(elements.trendChart, "Audit workbook unavailable.");
    setEmptyChart(elements.companyChart, "Audit workbook unavailable.");
    setEmptyChart(elements.categoryStackChart, "Audit workbook unavailable.");
    setEmptyChart(elements.categoryDetailChart, "Audit workbook unavailable.");
    setEmptyChart(elements.shareTotalChart, "Audit workbook unavailable.");
    setEmptyChart(elements.shareMonthChart, "Audit workbook unavailable.");
  }
}

function bindEvents() {
  if (elements.year) {
    elements.year.addEventListener("change", () => {
      state.filters.year = elements.year.value;
      renderAll();
    });
  }
  if (elements.month) {
    elements.month.addEventListener("change", () => {
      state.filters.month = elements.month.value;
      renderAll();
    });
  }
  if (elements.company) {
    elements.company.addEventListener("change", () => {
      state.filters.company = elements.company.value;
      renderAll();
    });
  }
  if (elements.category) {
    elements.category.addEventListener("change", () => {
      state.filters.category = elements.category.value;
      renderAll();
    });
  }
  if (elements.analysisCategory) {
    elements.analysisCategory.addEventListener("change", () => {
      state.analysisCategory = elements.analysisCategory.value;
      renderAll();
    });
  }
  if (elements.reset) {
    elements.reset.addEventListener("click", () => {
      state.filters = { year: "all", month: "all", company: "all", category: "all" };
      state.search = "";
      state.sort = { column: "_audit_month_sort", direction: "asc" };
      if (elements.search) {
        elements.search.value = "";
      }
      if (elements.year) {
        elements.year.value = "all";
      }
      if (elements.month) {
        elements.month.value = "all";
      }
      if (elements.company) {
        elements.company.value = "all";
      }
      if (elements.category) {
        elements.category.value = "all";
      }
      syncAnalysisCategoryControl();
      renderAll();
    });
  }
  if (elements.search) {
    elements.search.addEventListener("input", () => {
      state.search = elements.search.value || "";
      renderTable(getFilteredRows());
    });
  }
  if (elements.exportCsv) {
    elements.exportCsv.addEventListener("click", exportCurrentTable);
  }
  if (elements.tableHead) {
    elements.tableHead.addEventListener("click", (event) => {
      const button = event.target.closest("[data-sort-column]");
      if (!button) {
        return;
      }
      const column = button.getAttribute("data-sort-column");
      if (!column) {
        return;
      }
      if (state.sort.column === column) {
        state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
      } else {
        state.sort = { column, direction: "asc" };
      }
      renderTable(getFilteredRows());
    });
  }
}

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  loadAuditData();
});
