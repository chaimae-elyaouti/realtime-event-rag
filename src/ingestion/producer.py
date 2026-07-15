import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuration réseau centralisée
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "industrial-incidents"

COMPONENTS = ["High-Pressure Turbine", "Main Fan Bearing", "Fuel Delivery Pump", "Axial Compressor"]
ERROR_POOL = [
    {"code": "ERR_EGT_X203", "severity": "CRITICAL", "desc": "Exhaust Gas Temperature exceeded safety limits of 950°C. Current reading: {}°C. Threat of material thermal creep."},
    {"code": "ERR_VIB_B102", "severity": "WARNING", "desc": "Sustained high-frequency vibration spike detected on main shaft housing. Amplitude: {} mm/s."},
    {"code": "ERR_PR_P405", "severity": "CRITICAL", "desc": "Fuel actuator rail pressure dropped unexpectedly below {} bar. Risk of flameout conditions."},
    {"code": "ERR_STALL_C08", "severity": "CRITICAL", "desc": "Axial airflow velocity ratio mismatch detected. Stage 3 compressor blade aerodynamics unstable. Reading: {} ratio."}
]

def delivery_report(err, msg):
    """Callback asynchrone pour valider l'écriture dans Kafka sans perte."""
    if err is not None:
        print(f"[ENGINEERING ALERT] Message delivery failed: {err}")
    else:
        print(f"[DISPATCHER] Event safely committed to Topic [{msg.topic()}] Partition [{msg.partition()}]")

def generate_factory_incident(unit_id: int, cycle: int) -> dict:
    """Génère un log synthétique basé sur des modèles physiques d'usure."""
    fault = random.choice(ERROR_POOL)
    component = random.choice(COMPONENTS)
    
    if "X203" in fault["code"]:
        detail = round(random.uniform(951.0, 1020.0), 1)
    elif "B102" in fault["code"]:
        detail = round(random.uniform(4.2, 8.5), 2)
    elif "P405" in fault["code"]:
        detail = round(random.uniform(12.0, 18.5), 1)
    else:
        detail = round(random.uniform(0.65, 0.79), 2)

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "unit_id": unit_id,
        "cycle": cycle,
        "error_code": fault["code"],
        "component": component,
        "severity": fault["severity"],
        "log_message": fault["desc"].format(detail)
    }

def main():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': 'factory_incident_simulator',
        'acks': '1',                          # Garantie d'écriture minimale
        'queue.buffering.max.messages': 5000, 
        'compression.type': 'gzip'            
    }

    print(" Initializing Factory Incident Producer...")
    producer = Producer(conf)

    current_cycle = 1
    engine_id = 42

    try:
        while True:
            payload = generate_factory_incident(unit_id=engine_id, cycle=current_cycle)
            serialized_payload = json.dumps(payload).encode('utf-8')
            
            producer.produce(
                topic=TOPIC_NAME, 
                value=serialized_payload, 
                callback=delivery_report
            )
            producer.poll(0)
            
            print(f" [PRODUCER] Dispatched data cycle {current_cycle} for Engine #{engine_id}")
            
            current_cycle += 1
            if current_cycle > 150:
                current_cycle = 1
                engine_id = random.randint(1, 100)
                
            time.sleep(3) 
            
    except KeyboardInterrupt:
        print("\n Intercepted shutdown command. Flushing memory pipelines...")
    finally:
        producer.flush(timeout=5)
        print(" Producer pipeline safely closed.")

if __name__ == "__main__":
    main()