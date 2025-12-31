const API = "http://127.0.0.1:8000";
const charts = {};

function createGradient(ctx, chartArea, baseColor = "#4A90E2") {
  const gradient = ctx.createLinearGradient(
    0,
    chartArea.bottom,
    0,
    chartArea.top
  );
  gradient.addColorStop(0, baseColor + "33");
  gradient.addColorStop(0.5, baseColor + "66");
  gradient.addColorStop(1, baseColor + "CC");
  return gradient;
}

function drawChart(id, config) {
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(document.getElementById(id), config);
}

async function loadStations() {
  const res = await fetch(`${API}/stations`);
  const stations = await res.json();
  const sel = document.getElementById("station");
  stations.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = opt.text = s;
    sel.add(opt);
  });
  loadDashboard(stations[0]);
}

async function loadDashboard(station) {
  await loadTodaysAQI(station);
  await loadKPIs(station);
  await loadStationAQI();
  await loadMonthly(station);
  await loadSeasonal(station);
  await loadWeather(station);
  await loadUnhealthy(station);
  await loadForecast(station);
}

async function loadTodaysAQI(station) {
  const fRes = await fetch(`${API}/forecast?station=${station}`);
  const forecast = await fRes.json();

  const todayForecast = forecast.values[0] ?? "—";
  document.getElementById("todayPredicted").innerText = todayForecast;

  const wRes = await fetch(`${API}/weather?station=${station}`);
  const weatherData = await wRes.json();
  const actualAQI = weatherData.aqi.length
    ? weatherData.aqi[weatherData.aqi.length - 1]
    : "—";
  document.getElementById("todayActual").innerText = actualAQI;
}

async function loadKPIs(station) {
  const res = await fetch(`${API}/kpis?station=${station}`);
  const k = await res.json();
  document.getElementById("avgAQI").innerText = k.avg_aqi;
  document.getElementById("maxAQI").innerText = k.max_aqi;
  document.getElementById("unhealthyPct").innerText = k.unhealthy_pct + "%";
  document.getElementById("worstSeason").innerText = k.worst_season;
}

async function loadStationAQI() {
  const res = await fetch(`${API}/station_aqi`);
  const d = await res.json();
  drawChart("stationAqi", {
    type: "bar",
    data: {
      labels: d.stations,
      datasets: [
        {
          label: "Average AQI",
          data: d.aqi,
          backgroundColor: "#4A90E2",
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "AQI" } },
        x: { title: { display: true, text: "Station" } },
      },
    },
  });
}

async function loadMonthly(station) {
  const res = await fetch(`${API}/monthly?station=${station}&year=2024`);
  const d = await res.json();
  const ctx = document.getElementById("monthlyAqi").getContext("2d");
  drawChart("monthlyAqi", {
    type: "line",
    data: {
      labels: d.labels,
      datasets: [
        {
          label: "AQI",
          data: d.values,
          borderColor: "#4A90E2",
          backgroundColor: createGradient(
            ctx,
            ctx.canvas.getBoundingClientRect()
          ),
          tension: 0.35,
          pointRadius: 4,
          fill: true,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "AQI" } },
        x: { title: { display: true, text: "Month" } },
      },
    },
  });
}

async function loadSeasonal(station) {
  const res = await fetch(`${API}/seasonal?station=${station}`);
  const d = await res.json();
  drawChart("seasonalBar", {
    type: "bar",
    data: {
      labels: d.seasons,
      datasets: [
        { label: "PM2.5", data: d.pm25, backgroundColor: "#4A90E2" },
        { label: "PM10", data: d.pm10, backgroundColor: "#357ABD" },
        { label: "AQI", data: d.aqi, backgroundColor: "#A0C4FF" },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: "Concentration / AQI" },
        },
        x: { title: { display: true, text: "Season" } },
      },
    },
  });
}

async function loadWeather(station) {
  const res = await fetch(`${API}/weather?station=${station}`);
  const d = await res.json();
  drawChart("weatherImpact", {
    type: "scatter",
    data: {
      datasets: [
        {
          label: "Temperature vs AQI",
          data: d.temperature.map((t, i) => ({ x: t, y: d.aqi[i] })),
          backgroundColor: "#4A90E2",
        },
        {
          label: "Humidity vs AQI",
          data: d.humidity.map((h, i) => ({ x: h, y: d.aqi[i] })),
          backgroundColor: "#357ABD",
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "AQI" } },
        x: { title: { display: true, text: "Temperature / Humidity" } },
      },
    },
  });
}

async function loadUnhealthy(station) {
  const res = await fetch(`${API}/unhealthy_days?station=${station}`);
  const d = await res.json();
  drawChart("unhealthyTimeline", {
    type: "line",
    data: {
      labels: d.labels,
      datasets: [
        {
          label: "Unhealthy Days",
          data: d.values,
          borderColor: "#4A90E2",
          backgroundColor: "#4A90E233",
          tension: 0.3,
          pointRadius: 4,
          fill: true,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "Days" } },
        x: { title: { display: true, text: "Month-Year" } },
      },
    },
  });
}

async function loadForecast(station) {
  const res = await fetch(`${API}/forecast?station=${station}`);
  const d = await res.json();
  const ctx = document.getElementById("aqiForecast").getContext("2d");
  drawChart("aqiForecast", {
    type: "line",
    data: {
      labels: d.labels,
      datasets: [
        {
          label: "Predicted AQI",
          data: d.values,
          borderColor: "#4A90E2",
          backgroundColor: createGradient(
            ctx,
            ctx.canvas.getBoundingClientRect()
          ),
          tension: 0.35,
          pointRadius: 4,
          fill: true,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, title: { display: true, text: "AQI" } },
        x: { title: { display: true, text: "Date" } },
      },
    },
  });
}

document
  .getElementById("station")
  .addEventListener("change", (e) => loadDashboard(e.target.value));
loadStations();
