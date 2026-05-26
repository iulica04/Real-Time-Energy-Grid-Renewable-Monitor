"""
consumer_balancing.py
=====================
Consumer Group: balancing
Subscribed to: energy.production + energy.consumption

Scop:
  Calculează în timp real dacă o zonă are surplus sau deficit de energie
  regenerabilă față de consum. Aceste date sunt critice pentru operatorii
  de sistem (TSO - Transmission System Operator).

  surplus = total_renewable_mw - consumption_mw
  > 0 → surplus (poți exporta sau stoca)
  < 0 → deficit (trebuie import sau backup termic)

Sink: output/balancing_log.jsonl  (JSON Lines format)
"""

import json
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

# ─── Config ──────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
SINK_FILE = "output/balancing_log.jsonl"

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "balancing",                    # Consumer group ID
    "auto.offset.reset": "earliest",            # Citește de la început dacă nu există offset salvat
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}

# Buffer pentru a păstra ultima valoare per zonă din fiecare topic
latest = defaultdict(dict)  # {"RO": {"production": {...}, "consumption": {...}}}

# ─── Balancing logic ──────────────────────────────────────────────────────────

def compute_balance(zone: str) -> dict | None:
    """
    Returnează balanța dacă avem date din ambele topic-uri pentru zona dată.
    """
    zone_data = latest[zone]
    if "production" not in zone_data or "consumption" not in zone_data:
        return None

    prod = zone_data["production"]
    cons = zone_data["consumption"]

    surplus_mw = round(prod["total_renewable_mw"] - cons["consumption_mw"], 1)
    renewable_share = round(
        prod["total_renewable_mw"] / max(cons["consumption_mw"], 1) * 100, 1
    )

    return {
        "zone":                   zone,
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "renewable_generation_mw": prod["total_renewable_mw"],
        "consumption_mw":          cons["consumption_mw"],
        "surplus_deficit_mw":      surplus_mw,
        "renewable_share_pct":     renewable_share,
        "status":                  "SURPLUS" if surplus_mw >= 0 else "DEFICIT",
        "solar_mw":                prod["solar_mw"],
        "wind_mw":                 prod["wind_mw"],
        "spot_price_eur_mwh":      cons["spot_price_eur_mwh"],
    }

# ─── Main consumer loop ───────────────────────────────────────────────────────

def run_consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(["energy.production", "energy.consumption"])

    print(f"[balancing] Consumer started | group.id='balancing'")
    print(f"[balancing] Subscribed to: energy.production, energy.consumption")
    print(f"[balancing] Sink: {SINK_FILE}\n")

    with open(SINK_FILE, "a") as sink:
        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"[balancing] EOF partition {msg.partition()}, offset {msg.offset()}")
                    else:
                        print(f"[balancing] ERROR: {msg.error()}")
                    continue

                zone  = msg.key().decode("utf-8")
                data  = json.loads(msg.value().decode("utf-8"))
                topic = msg.topic()

                print(f"[balancing] Received from {topic} | zone={zone} "
                      f"partition={msg.partition()} offset={msg.offset()}")

                # Salvăm ultima valoare în buffer
                if topic == "energy.production":
                    latest[zone]["production"] = data
                elif topic == "energy.consumption":
                    latest[zone]["consumption"] = data

                # Calculăm balanța dacă avem ambele date
                balance = compute_balance(zone)
                if balance:
                    status_icon = "✅ SURPLUS" if balance["status"] == "SURPLUS" else "⚠️  DEFICIT"
                    print(f"  [{zone}] {status_icon} "
                          f"{balance['surplus_deficit_mw']:+.1f} MW | "
                          f"renewable={balance['renewable_share_pct']}% | "
                          f"price={balance['spot_price_eur_mwh']}€/MWh")

                    # Scriem în sink
                    sink.write(json.dumps(balance) + "\n")
                    sink.flush()

        except KeyboardInterrupt:
            print("\n[balancing] Consumer stopped.")
        finally:
            consumer.close()

if __name__ == "__main__":
    run_consumer()
