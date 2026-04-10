import os
import json
import uuid
import threading
from datetime import datetime, timezone
import uvicorn
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer, KafkaConsumer

app = FastAPI(title="CinemaAbyss Events Service")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKERS"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)


def run_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=os.getenv("KAFKA_BROKERS"),
            group_id='events-service-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )

        print(f"Consumer started. Topic: {KAFKA_TOPIC}")

        for message in consumer:
            print(f"Consumer got message: {message.value.get('id')}")

            event = message.value
            event_type = event.get('type', 'unknown')
            event_id = event.get('id', 'unknown')
            payload = event.get('payload', {})

            if event_type == 'movie':
                print(
                    f"[MOVIE] ID:{event_id} | Action:{payload.get('action')} | Title:{payload.get('title')}")

            elif event_type == 'user':
                print(
                    f"[USER] ID:{event_id} | Action:{payload.get('action')} | User:{payload.get('username')}")

            elif event_type == 'payment':
                print(
                    f"[PAYMENT] ID:{event_id} | Amount:{payload.get('amount')} | Status:{payload.get('status')}")

            else:
                print(f"[UNKNOWN] ID:{event_id} | Type:{event_type}")
    except Exception as e:
        print(f"Consumer failed to start: {e}")


consumer_thread = threading.Thread(target=run_consumer, daemon=True)
consumer_thread.start()


def send_to_kafka(event_type: str, payload: dict) -> tuple:
    event = {
        "id": f"{event_type}-{uuid.uuid4().hex[:8]}",
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    future = producer.send(KAFKA_TOPIC, key=event_type, value=event)
    record_metadata = future.get(timeout=10)

    print(
        f"Kafka PRODUCED: {event['id']} | type={event_type} | partition={record_metadata.partition} | offset={record_metadata.offset}")

    return record_metadata.partition, record_metadata.offset, event


@app.get("/api/events/health")
def events_health():
    return {"status": True}


@app.post("/api/events/movie", status_code=201)
def create_movie_event(event: dict):
    required = ["movie_id", "title", "action"]
    if not all(k in event for k in required):
        raise HTTPException(
            status_code=400, detail=f"Missing required fields: {required}")

    try:
        partition, offset, full_event = send_to_kafka("movie", event)
        return {
            "status": "success",
            "partition": partition,
            "offset": offset,
            "event": full_event,
        }
    except Exception as e:
        print(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")


@app.post("/api/events/user", status_code=201)
def create_user_event(event: dict):
    required = ["user_id", "action", "timestamp"]
    if not all(k in event for k in required):
        raise HTTPException(
            status_code=400, detail=f"Missing required fields: {required}")

    try:
        partition, offset, full_event = send_to_kafka("user", event)
        return {
            "status": "success",
            "partition": partition,
            "offset": offset,
            "event": full_event,
        }
    except Exception as e:
        print(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")


@app.post("/api/events/payment", status_code=201)
def create_payment_event(event: dict):
    required = ["payment_id", "user_id", "amount", "status", "timestamp"]
    if not all(k in event for k in required):
        raise HTTPException(
            status_code=400, detail=f"Missing required fields: {required}")

    try:
        partition, offset, full_event = send_to_kafka("payment", event)
        return {
            "status": "success",
            "partition": partition,
            "offset": offset,
            "event": full_event,
        }
    except Exception as e:
        print(f"Kafka error: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8082)))
