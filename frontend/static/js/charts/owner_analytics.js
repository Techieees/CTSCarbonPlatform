(function () {
  const payloadNode = document.getElementById("owner-analytics-chart-data");
  if (!payloadNode || typeof echarts === "undefined") return;

  let payload = {};
  try {
    payload = JSON.parse(payloadNode.textContent || "{}");
  } catch (error) {
    payload = {};
  }

  function barOption(title, rows) {
    return {
      animationDuration: 400,
      tooltip: { trigger: "axis" },
      grid: { left: 42, right: 16, top: 28, bottom: 42, containLabel: true },
      xAxis: {
        type: "category",
        data: rows.map((item) => item.name),
        axisLabel: { interval: 0, rotate: rows.length > 5 ? 28 : 0 },
      },
      yAxis: { type: "value" },
      series: [{
        type: "bar",
        data: rows.map((item) => item.value),
        itemStyle: { color: "#2f5fb3", borderRadius: [6, 6, 0, 0] },
      }],
      title: { text: title, show: false },
    };
  }

  function lineOption(rows) {
    return {
      animationDuration: 400,
      tooltip: { trigger: "axis" },
      grid: { left: 42, right: 16, top: 28, bottom: 42, containLabel: true },
      xAxis: { type: "category", data: rows.map((item) => item.name) },
      yAxis: { type: "value" },
      series: [{
        type: "line",
        smooth: true,
        data: rows.map((item) => item.value),
        lineStyle: { width: 3, color: "#1f9d55" },
        itemStyle: { color: "#1f9d55" },
        areaStyle: { color: "rgba(31,157,85,0.12)" },
      }],
    };
  }

  function pieOption(rows) {
    return {
      animationDuration: 400,
      tooltip: { trigger: "item" },
      legend: { bottom: 0, left: "center" },
      series: [{
        type: "pie",
        radius: ["42%", "72%"],
        avoidLabelOverlap: true,
        data: rows.map((item) => ({ name: item.name, value: item.value })),
      }],
    };
  }

  const charts = [
    ["ownerChartDailyActive", lineOption(payload.daily_active_users || [])],
    ["ownerChartHour", barOption("Activity by hour", payload.activity_by_hour || [])],
    ["ownerChartPages", barOption("Top pages", payload.top_pages || [])],
    ["ownerChartCountries", pieOption(payload.country_distribution || [])],
    ["ownerChartBrowsers", pieOption(payload.browser_distribution || [])],
    ["ownerChartCompanies", barOption("Companies", payload.company_distribution || [])],
    ["ownerChartFeatures", barOption("Features", payload.feature_usage || [])],
    ["ownerChartDatasets", barOption("Datasets", payload.dataset_usage || [])],
    ["ownerChartSessions", barOption("Session duration", payload.session_duration_distribution || [])],
  ];

  const instances = charts.map(([id, option]) => {
    const node = document.getElementById(id);
    if (!node) return null;
    const chart = echarts.init(node);
    chart.setOption(option);
    return chart;
  }).filter(Boolean);

  window.addEventListener("resize", function () {
    instances.forEach((chart) => chart.resize());
  });
})();
