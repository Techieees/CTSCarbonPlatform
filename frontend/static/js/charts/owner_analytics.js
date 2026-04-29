(function () {
  const payloadNode = document.getElementById("owner-analytics-chart-data");
  if (!payloadNode || typeof echarts === "undefined") return;

  let payload = {};
  try {
    payload = JSON.parse(payloadNode.textContent || "{}");
  } catch (error) {
    payload = {};
  }

  const axisLabel = {
    color: "#0f172a",
    fontSize: 12,
    fontWeight: 700,
  };

  function chartHeight(rows, minimum) {
    return Math.max(minimum || 520, rows.length * 42 + 120);
  }

  function barOption(title, rows) {
    const useHorizontalBars = rows.length > 4 || rows.some((item) => String(item.name || "").length > 14);
    if (useHorizontalBars) {
      return {
        ownerHeight: chartHeight(rows, 560),
        animationDuration: 250,
        tooltip: { trigger: "axis" },
        grid: { left: 190, right: 28, top: 28, bottom: 28, containLabel: false },
        xAxis: {
          type: "value",
          axisLabel,
        },
        yAxis: {
          type: "category",
          inverse: true,
          data: rows.map((item) => item.name),
          axisLabel: {
            ...axisLabel,
            width: 170,
            overflow: "truncate",
          },
        },
        series: [{
          type: "bar",
          data: rows.map((item) => item.value),
          itemStyle: { color: "#2f5fb3", borderRadius: [0, 6, 6, 0] },
          barMaxWidth: 28,
        }],
        title: { text: title, show: false },
      };
    }

    return {
      ownerHeight: 520,
      animationDuration: 250,
      tooltip: { trigger: "axis" },
      grid: { left: 52, right: 24, top: 28, bottom: 72, containLabel: true },
      xAxis: {
        type: "category",
        data: rows.map((item) => item.name),
        axisLabel: { ...axisLabel, interval: 0, rotate: rows.length > 3 ? 35 : 0 },
      },
      yAxis: { type: "value", axisLabel },
      series: [{
        type: "bar",
        data: rows.map((item) => item.value),
        itemStyle: { color: "#2f5fb3", borderRadius: [6, 6, 0, 0] },
        barMaxWidth: 34,
      }],
      title: { text: title, show: false },
    };
  }

  function lineOption(rows) {
    return {
      ownerHeight: 520,
      animationDuration: 250,
      tooltip: { trigger: "axis" },
      grid: { left: 52, right: 24, top: 28, bottom: 72, containLabel: true },
      xAxis: { type: "category", data: rows.map((item) => item.name), axisLabel: { ...axisLabel, interval: 0, rotate: rows.length > 7 ? 35 : 0 } },
      yAxis: { type: "value", axisLabel },
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
      ownerHeight: 520,
      animationDuration: 250,
      tooltip: { trigger: "item" },
      legend: {
        bottom: 0,
        left: "center",
        type: "scroll",
        textStyle: axisLabel,
      },
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
    ["ownerChartCities", barOption("Cities", payload.city_distribution || [])],
    ["ownerChartBrowsers", pieOption(payload.browser_distribution || [])],
    ["ownerChartDevices", pieOption(payload.device_distribution || [])],
    ["ownerChartOS", pieOption(payload.os_distribution || [])],
    ["ownerChartCompanies", barOption("Companies", payload.company_distribution || [])],
    ["ownerChartFeatures", barOption("Features", payload.feature_usage || [])],
    ["ownerChartActions", barOption("Actions", payload.action_distribution || [])],
    ["ownerChartReferrers", barOption("Referrers", payload.referrer_distribution || [])],
    ["ownerChartDatasets", barOption("Datasets", payload.dataset_usage || [])],
    ["ownerChartSessions", barOption("Session duration", payload.session_duration_distribution || [])],
  ];

  const instances = charts.map(([id, option]) => {
    const node = document.getElementById(id);
    if (!node) return null;
    const chart = echarts.init(node);
    const { ownerHeight, ...echartsOption } = option;
    if (ownerHeight) {
      node.style.minHeight = ownerHeight + "px";
      node.style.height = ownerHeight + "px";
    }
    chart.setOption(echartsOption);
    if (ownerHeight) {
      chart.resize({ height: ownerHeight });
    }
    return chart;
  }).filter(Boolean);

  window.addEventListener("resize", function () {
    instances.forEach((chart) => chart.resize());
  });
})();
