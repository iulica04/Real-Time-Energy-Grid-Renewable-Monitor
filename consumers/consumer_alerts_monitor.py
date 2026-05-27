"""
consumer_alerts_monitor.py
===========================
Consumer Group : alerts-monitor
Subscribed to  : energy.alerts

Scop:
  Centralizează toate alertele din sistem într-un singur loc.
  Alertele pot veni din surse multiple care produc în energy.alerts:
    - consumer_renewable_report.py  → LOW_RENEWABLE_OUTPUT, WIND_DOMINANT, SOLAR_DOMINANT
    - (viitor) consumer din energy.consumption → HIGH_PRICE, HIGH_DEMAND

  La fiecare SUMMARY_EVERY alerte afișează un rezumat complet
  cu counter per zonă și per tip de alertă.

Sink: output/alerts_monitor.jsonl
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

# ─── Config ───────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
SOURCE_TOPIC      = "energy.alerts"
SINK_FILE         = "output/alerts_monitor.jsonl"

SUMMARY_EVERY     = 10   # afișează summary la fiecare N alerte

consumer_config = {
    "bootstrap.servers":       BOOTSTRAP_SERVERS,
    "group.id":                "alerts-monitor",
    "auto.offset.reset":       "latest",
    "enable.auto.commit":      True,
    "auto.commit.interval.ms": 5000,
}

# ─── State ────────────────────────────────────────────────────────────────────

total_alerts              = 0
counter_per_zone          = defaultdict(int)          # { "RO": 3, "DE": 1 }
counter_per_type          = defaultdict(int)          # { "LOW_RENEWABLE_OUTPUT": 2 }
counter_per_zone_and_type = defaultdict(lambda: defaultdict(int))  # { "RO": { "LOW_RENEWABLE_OUTPUT": 2 } }

# ─── Summary ──────────────────────────────────────────────────────────────────

def print_summary():
    print("\n" + "=" * 52)
    print(f"  ALERT SUMMARY  |  total={total_alerts} alerte")
    print("=" * 52)

    print("  Per zona:")
    for zone, count in sorted(counter_per_zone.items(), key=lambda x: -x[1]):
        types = counter_per_zone_and_type[zone]
        types_str = " | ".join(f"{t}={c}" for t, c in sorted(types.items()))
        print(f"    {zone:<10} {count:>3}x  ({types_str})")

    print("  Per tip:")
    for alert_type, count in sorted(counter_per_type.items(), key=lambda x: -x[1]):
        print(f"    {alert_type:<30} {count:>3}x")

    print("=" * 52 + "\n")

# ─── Main ─────────────────────────────────────────────────────────────────────

def run_consumer():
    global total_alerts

    consumer = Consumer(consumer_config)
    consumer.subscribe([SOURCE_TOPIC])

    print("[alerts-monitor] Consumer started")
    print(f"[alerts-monitor] group.id='alerts-monitor'")
    print(f"[alerts-monitor] Subscribed to : {SOURCE_TOPIC}")
    print(f"[alerts-monitor] Sink          : {SINK_FILE}")
    print(f"[alerts-monitor] Summary every : {SUMMARY_EVERY} alerte\n")

    with open(SINK_FILE, "a") as sink:
        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[alerts-monitor] ERROR: {msg.error()}")
                    continue

                zone       = msg.key().decode("utf-8")
                data       = json.loads(msg.value().decode("utf-8"))
                partition  = msg.partition()
                offset     = msg.offset()
                alert_type = data.get("alert_type", "UNKNOWN")
                source     = data.get("source_topic", "unknown")

                # Actualizează contoarele
                total_alerts += 1
                counter_per_zone[zone] += 1
                counter_per_type[alert_type] += 1
                counter_per_zone_and_type[zone][alert_type] += 1

                # Afișează în consolă
                print(
                    f"[alerts-monitor] [{alert_type}] "
                    f"zone={zone} | "
                    f"source={source} | "
                    f"partition={partition} offset={offset} | "
                    f"total={total_alerts}"
                )

                # Detalii extra din payload
                if "total_renewable_mw" in data:
                    print(
                        f"  renewable={data['total_renewable_mw']}MW | "
                        f"capacity={data.get('total_capacity_factor_pct')}% | "
                        f"dominant={data.get('dominant_source')}"
                    )
                if "spot_price_eur_mwh" in data:
                    print(
                        f"  price={data['spot_price_eur_mwh']}EUR/MWh | "
                        f"consumption={data.get('consumption_mw')}MW"
                    )

                # Scrie în sink
                record = {
                    **data,
                    "received_at":    datetime.now(timezone.utc).isoformat(),
                    "kafka_partition": partition,
                    "kafka_offset":    offset,
                }
                sink.write(json.dumps(record) + "\n")
                sink.flush()

                # Summary periodic
                if total_alerts % SUMMARY_EVERY == 0:
                    print_summary()

        except KeyboardInterrupt:
            print("\n[alerts-monitor] Consumer oprit.")
            print_summary()
        finally:
            consumer.close()


if __name__ == "__main__":
    run_consumer()
