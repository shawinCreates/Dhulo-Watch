async function predictAQI() {
  const payload = {
    pm2_5: parseFloat(document.getElementById("pm2_5").value),
    pm10: parseFloat(document.getElementById("pm10").value),
    no2: parseFloat(document.getElementById("no2").value),
    so2: parseFloat(document.getElementById("so2").value),
    o3: parseFloat(document.getElementById("o3").value),
    temperature_C: parseFloat(document.getElementById("temp").value),
    relative_humidity_: parseFloat(document.getElementById("rh").value),
    month_sin: Math.sin((2 * Math.PI * new Date().getMonth()) / 12),
    month_cos: Math.cos((2 * Math.PI * new Date().getMonth()) / 12),
    dow_sin: Math.sin((2 * Math.PI * new Date().getDay()) / 7),
    dow_cos: Math.cos((2 * Math.PI * new Date().getDay()) / 7),
  };

  const response = await fetch("http://127.0.0.1:8000/predict", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  document.getElementById(
    "result"
  ).innerText = `Predicted AQI: ${data.predicted_aqi}`;
}
