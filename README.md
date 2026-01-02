# Dhulo Watch  
**Monitoring Nepal’s air, one breath at a time.**

> A production-oriented, full-stack machine learning project focused on air quality (AQI) analysis and forecasting for Nepal.

---

## 🔍 Why This Project Matters

Air pollution is a growing public health challenge in Nepal. **Dhulo Watch** demonstrates how data engineering, machine learning, and modern backend development can be combined to transform raw environmental data into actionable forecasts and visual insights.

This project was designed and built end-to-end to reflect **real-world ML system design**, not just isolated model training.

---

## 🧠 What I Built (At a Glance)

- Designed a complete ML pipeline: **data → model → API → frontend**
- Trained and evaluated multiple regression models for AQI forecasting
- Deployed a **FastAPI backend** for real-time inference
- Built a frontend dashboard to visualize historical and predicted AQI
- Packaged trained models for **production-ready inference**
- Documented experiments using reproducible notebooks

---

## 🚀 Key Features

- Multi-step AQI forecasting using supervised regression
- RESTful API for predictions and time-series data
- Pre-trained model for immediate deployment
- Clean separation of data, model, backend, and UI layers
- Lightweight frontend optimized for clarity and speed

---

## 🛠️ Tech Stack

### Backend & Machine Learning
- Python
- FastAPI
- Scikit-learn
- Pandas, NumPy
- Uvicorn

### Modeling
- Random Forest Regressor
- XGBoost Regressor
- LightGBM

### Frontend
- HTML
- CSS
- Vanilla JavaScript

### Tooling
- Jupyter Notebook
- Git & GitHub

---

## ⚙️ How to Run Locally

### Backend

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```
---
## 📊 Machine Learning Details
- Framed AQI prediction as a time-series regression problem
- Performed exploratory data analysis and feature preprocessing
- Evaluated multiple tree-based regression models
- Selected the best-performing model based on validation metrics
- Serialized the final model for production inference
- Integrated predictions into a REST API
---
## 🛣️ Future Enhancements
- Real-time AQI ingestion from public APIs
- Map-based and interactive visualizations
- Model explainability (SHAP)
- Dockerized deployment
- Cloud hosting (AWS / GCP / Azure)
