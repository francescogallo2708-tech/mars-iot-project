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

INGESTION_SERVICE_URL = "http://ingestion-service:8000"

@app.get("/")
def root():
    return {"message": "Rule engine is running"}


@app.get("/normalized-events")
def get_normalized_events():
    response = requests.get(f"{INGESTION_SERVICE_URL}/normalized-sensor-data")
    return response.json()

@app.get("/evaluate-rules")
@app.get("/evaluate-rules")
@app.get("/evaluate-rules")
def evaluate_rules():
    response = requests.get(f"{INGESTION_SERVICE_URL}/normalized-sensor-data")
    events = response.json()

    db = SessionLocal()
    rules = db.query(Rule).all()

    triggered_actions = []

    for event in events:
        for rule in rules:
            if event["sensor_id"] == rule.sensor_id and event["metric"] == rule.metric:
                condition_met = False

                if rule.operator == ">":
                    condition_met = event["value"] > rule.threshold
                elif rule.operator == "<":
                    condition_met = event["value"] < rule.threshold
                elif rule.operator == ">=":
                    condition_met = event["value"] >= rule.threshold
                elif rule.operator == "<=":
                    condition_met = event["value"] <= rule.threshold
                elif rule.operator == "=":
                    condition_met = event["value"] == rule.threshold

                if condition_met:
                    actuator_response = requests.post(
                        f"http://simulator:8080/api/actuators/{rule.actuator}",
                        json={"state": rule.state}
                    )

                    triggered_actions.append({
                        "rule_id": rule.id,
                        "rule": f"IF {rule.sensor_id} {rule.operator} {rule.threshold} THEN {rule.actuator} = {rule.state}",
                        "action": {
                            "actuator": rule.actuator,
                            "state": rule.state
                        },
                        "event": event,
                        "actuator_result": actuator_response.json()
                    })

    return triggered_actions

from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = "sqlite:///rules.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

class Rule(Base):
    __tablename__ = "rules"

    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String)
    metric = Column(String)
    operator = Column(String)
    threshold = Column(Float)
    actuator = Column(String)
    state = Column(String)

Base.metadata.create_all(bind=engine)

@app.post("/rules")
def create_rule(rule: dict):

    db = SessionLocal()

    new_rule = Rule(
        sensor_id=rule["sensor_id"],
        metric=rule["metric"],
        operator=rule["operator"],
        threshold=rule["threshold"],
        actuator=rule["actuator"],
        state=rule["state"]
    )

    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)

    return new_rule

@app.get("/rules")
def get_rules():

    db = SessionLocal()
    rules = db.query(Rule).all()

    return rules

@app.get("/actuators")
def get_actuators():
    response = requests.get("http://simulator:8080/api/actuators")
    return response.json()