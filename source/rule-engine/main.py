from fastapi import FastAPI, HTTPException
import requests
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import stomp
import json
from threading import Thread

app = FastAPI()

# --- 1. CONFIGURATION & MIDDLEWARE ---
# Enable CORS so your frontend at localhost:3000 can talk to this service
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. DATABASE SETUP (Requirement 4.1: Persistence) ---
# This ensures your automation rules survive a container restart
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

# --- 3. BROKER LISTENER (Requirement 6: Event-Driven Architecture) ---
# This listener reacts instantly when the Ingestion Service pushes data to the broker 
class RuleEngineListener(stomp.ConnectionListener):
    def on_message(self, frame):
        try:
            # 1. Parse the Unified Internal Event from the broker
            event = json.loads(frame.body)
            sensor_id = event.get("sensor_id")
            value = event.get("value")
            
            print(f"DEBUG: Processing event from {sensor_id}: {value}")

            # 2. Query the database for rules matching this sensor
            db = SessionLocal()
            rules = db.query(Rule).filter(Rule.sensor_id == sensor_id).all()

            for rule in rules:
                condition_met = False
                
                # 3. Evaluate the IF-THEN logic
                if rule.operator == ">": condition_met = value > rule.threshold
                elif rule.operator == "<": condition_met = value < rule.threshold
                elif rule.operator == ">=": condition_met = value >= rule.threshold
                elif rule.operator == "<=": condition_met = value <= rule.threshold
                elif rule.operator == "=": condition_met = value == rule.threshold

                if condition_met:
                    print(f"CRITICAL: Triggering {rule.actuator} to {rule.state}")
                    # 4. Invoke the Actuator REST API in the simulator
                    requests.post(
                        f"http://simulator:8080/api/actuators/{rule.actuator}",
                        json={"state": rule.state}
                    )
            db.close()
        except Exception as e:
            print(f"Error in Rule Listener: {e}")

def start_broker_subscriber():
    """Connects to the broker and starts listening in the background."""
    try:
        # 'broker' is the service name defined in your docker-compose.yml
        conn = stomp.Connection([('broker', 61613)])
        conn.set_listener('', RuleEngineListener())
        conn.connect('admin', 'admin', wait=True)
        # Subscribe to the topic used by the ingestion service
        conn.subscribe(destination='/topic/mars.telemetry', id=1, ack='auto')
        print("Rule Engine is successfully listening to the Broker.")
    except Exception as e:
        print(f"Failed to connect to broker: {e}")

# Start the background thread so FastAPI can still handle web requests
Thread(target=start_broker_subscriber, daemon=True).start()

# --- 4. REST API ENDPOINTS (Requirement 5.3: Frontend Dashboard) ---

@app.get("/")
def root():
    return {"message": "Rule Engine is running and listening to the broker."}

@app.post("/rules")
def create_rule(rule: dict):
    """Adds a new rule to the persistent SQLite database."""
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
    db.close()
    return new_rule

@app.get("/rules")
def get_rules():
    """Returns all persisted rules for the dashboard."""
    db = SessionLocal()
    rules = db.query(Rule).all()
    db.close()
    return rules

@app.get("/actuators")
def get_actuators():
    """Proxy request to see current actuator states from the simulator."""
    try:
        response = requests.get("http://simulator:8080/api/actuators")
        return response.json()
    except:
        return {"error": "Simulator unreachable"}