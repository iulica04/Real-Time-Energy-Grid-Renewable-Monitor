"""
consumer_storage.py
===================
Consumer Group: storage
Subscribed to: energy.production + energy.consumption

Scop:
  Consumă toate mesajele din ambele topic-uri și le stochează în fișiere
  CSV structurate (sau Parquet dacă pandas/pyarrow sunt disponibile).
  Acesta este sink-ul principal al proiectului.

Sink:
  output/production_YYYY-MM-DD.csv
  output/consumption_YYYY-MM-DD.csv

Notă despre offset:
  Offset-ul reprezintă poziția unui mesaj în cadrul unui partition.
  Kafka nu șterge mesajele după consum — le ține conform retention policy.
  Dacă oprim acest consumer și îl repornit, el continuă de la ultimul
  offset comis (committed offset), nu de la început.
  
  Pentru a face REPLAY (reciti toate datele de la început):
    auto.offset.reset = 'earliest'
  sau manual via CLI:
    kafka-consumer-groups --reset-offsets --to-earliest --group storage
"""

import json
import csv
import os
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

# ─── Config ──────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "storage",                     # Consumer group ID
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}

# ─── CSV helpers ─────────────────────────────────────────────────────────────

PROD_HEADERS = [
    "timestamp", "zone", "solar_mw", "wind_mw",
    "total_renewable_mw", "solar_capacity_factor", "wind_capacity_factor"
]
CONS_HEADERS = [
    "timestamp", "zone", "consumption_mw",
    "spot_price_eur_mwh", "demand_level"
]

# Buffere pentru batch write (evită I/O la fiecare mesaj)
prod_buffer = []
cons_buffer = []
BATCH_SIZE  = 10

def get_sink_path(prefix: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return f"output/{prefix}_{today}.csv"

def write_batch(buffer: list, headers: list, filepath: str):
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(buffer)

# ─── Main consumer loop ───────────────────────────────────────────────────────

def run_consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(["energy.production", "energy.consumption"])

    print(f"[storage] Consumer started | group.id='storage'")
    print(f"[storage] Subscribed to: energy.production, energy.consumption")
    print(f"[storage] Writing to: output/production_*.csv, output/consumption_*.csv\n")

    msg_count = defaultdict(int)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush dacă avem date în buffer
                if prod_buffer:
                    write_batch(prod_buffer, PROD_HEADERS, get_sink_path("production"))
                    print(f"[storage] Flushed {len(prod_buffer)} production records")
                    prod_buffer.clear()
                if cons_buffer:
                    write_batch(cons_buffer, CONS_HEADERS, get_sink_path("consumption"))
                    print(f"[storage] Flushed {len(cons_buffer)} consumption records")
                    cons_buffer.clear()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[storage] ERROR: {msg.error()}")
                continue

            zone  = msg.key().decode("utf-8")
            data  = json.loads(msg.value().decode("utf-8"))
            topic = msg.topic()
            msg_count[topic] += 1

            print(f"[storage] ← {topic} | zone={zone} "
                  f"partition={msg.partition()} offset={msg.offset()}")

            if topic == "energy.production":
                prod_buffer.append(data)
                if len(prod_buffer) >= BATCH_SIZE:
                    write_batch(prod_buffer, PROD_HEADERS, get_sink_path("production"))
                    prod_buffer.clear()

            elif topic == "energy.consumption":
                cons_buffer.append(data)
                if len(cons_buffer) >= BATCH_SIZE:
                    write_batch(cons_buffer, CONS_HEADERS, get_sink_path("consumption"))
                    cons_buffer.clear()

            # Status la fiecare 50 mesaje
            total = sum(msg_count.values())
            if total % 50 == 0:
                print(f"[storage] Total stored: "
                      f"production={msg_count['energy.production']} | "
                      f"consumption={msg_count['energy.consumption']}")

    except KeyboardInterrupt:
        print("\n[storage] Consumer stopping — flushing remaining data...")
        if prod_buffer:
            write_batch(prod_buffer, PROD_HEADERS, get_sink_path("production"))
        if cons_buffer:
            write_batch(cons_buffer, CONS_HEADERS, get_sink_path("consumption"))
        print("[storage] Done.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()
