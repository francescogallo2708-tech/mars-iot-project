# System Overview

The platform ingests heterogeneous sensor data from the Mars IoT simulator and normalizes them into a unified internal event format.

## Unified Internal Event Schema

Each normalized event follows this structure:

```json
{
  "sensor_id": "greenhouse_temperature",
  "timestamp": "2026-03-06T08:49:13.483660+00:00",
  "metric": "temperature_c",
  "value": 24.69,
  "unit": "C",
  "status": "ok"
}