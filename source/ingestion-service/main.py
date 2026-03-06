from fastapi import FastAPI
import requests
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SIMULATOR_BASE_URL = "http://simulator:8080"


@app.get("/")
def root():
    return {"message": "Ingestion service is running"}


@app.get("/sensors")
def get_sensors():
    response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors")
    return response.json()


@app.get("/sensor/{sensor_id}")
def get_sensor(sensor_id: str):
    response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors/{sensor_id}")
    return response.json()

@app.get("/sensor-data")
def get_all_sensor_data():
    sensors_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors")
    sensors_json = sensors_response.json()

    sensors = sensors_json["sensors"]

    results = []

    for sensor_id in sensors:
        sensor_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors/{sensor_id}")
        results.append({
            "sensor_id": sensor_id,
            "data": sensor_response.json()
        })

    return results

@app.get("/normalized-sensor-data")
def get_normalized_sensor_data():
    sensors_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors")
    sensors_json = sensors_response.json()
    sensors = sensors_json["sensors"]

    normalized_results = []

    for sensor_id in sensors:
        sensor_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors/{sensor_id}")
        data = sensor_response.json()

        if "metric" in data and "value" in data:
            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": data.get("metric"),
                "value": data.get("value"),
                "unit": data.get("unit"),
                "status": data.get("status")
            })

        elif "measurements" in data:
            for measurement in data["measurements"]:
                normalized_results.append({
                    "sensor_id": sensor_id,
                    "timestamp": data.get("captured_at"),
                    "metric": measurement.get("metric"),
                    "value": measurement.get("value"),
                    "unit": measurement.get("unit"),
                    "status": data.get("status")
                })

        elif "level_pct" in data:
            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": "level_pct",
                "value": data.get("level_pct"),
                "unit": "%",
                "status": data.get("status")
            })

            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": "level_liters",
                "value": data.get("level_liters"),
                "unit": "L",
                "status": data.get("status")
            })

        elif "pm25_ug_m3" in data:
            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": "pm1_ug_m3",
                "value": data.get("pm1_ug_m3"),
                "unit": "ug/m3",
                "status": data.get("status")
            })

            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": "pm25_ug_m3",
                "value": data.get("pm25_ug_m3"),
                "unit": "ug/m3",
                "status": data.get("status")
            })

            normalized_results.append({
                "sensor_id": sensor_id,
                "timestamp": data.get("captured_at"),
                "metric": "pm10_ug_m3",
                "value": data.get("pm10_ug_m3"),
                "unit": "ug/m3",
                "status": data.get("status")
            })

    return normalized_results

latest_sensor_state = {}

@app.get("/latest-state")
def get_latest_state():
    sensors_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors")
    sensors_json = sensors_response.json()
    sensors = sensors_json["sensors"]

    for sensor_id in sensors:
        sensor_response = requests.get(f"{SIMULATOR_BASE_URL}/api/sensors/{sensor_id}")
        data = sensor_response.json()

        if "metric" in data and "value" in data:
            latest_sensor_state[sensor_id] = {
                "timestamp": data.get("captured_at"),
                "metric": data.get("metric"),
                "value": data.get("value"),
                "unit": data.get("unit"),
                "status": data.get("status")
            }

        elif "measurements" in data:
            latest_sensor_state[sensor_id] = {
                "timestamp": data.get("captured_at"),
                "measurements": data.get("measurements"),
                "status": data.get("status")
            }

        elif "level_pct" in data:
            latest_sensor_state[sensor_id] = {
                "timestamp": data.get("captured_at"),
                "level_pct": data.get("level_pct"),
                "level_liters": data.get("level_liters"),
                "status": data.get("status")
            }

        elif "pm25_ug_m3" in data:
            latest_sensor_state[sensor_id] = {
                "timestamp": data.get("captured_at"),
                "pm1_ug_m3": data.get("pm1_ug_m3"),
                "pm25_ug_m3": data.get("pm25_ug_m3"),
                "pm10_ug_m3": data.get("pm10_ug_m3"),
                "status": data.get("status")
            }

    return latest_sensor_state



