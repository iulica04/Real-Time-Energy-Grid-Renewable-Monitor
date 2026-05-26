"""
Consumer Group: pricing-alerts
Subscribed to: energy.consumption

Scop:
  Monitorizează prețurile spot și generează alerte când prețul depășește
  praguri critice. Relevant pentru traderi și consumatori industriali.

Thresholds:
  > 120 EUR/MWh → HIGH alert
  > 80 EUR/MWh  → MEDIUM alert
  ≤ 80 EUR/MWh  → LOW / normal

Sink: output/pricing_alerts.jsonl

Notă despre Consumer Groups:
  Deoarece 'pricing-alerts' este un group.id DIFERIT față de 'balancing',
  Kafka îi oferă propria copie a mesajelor. Ambele grupuri citesc același topic
  INDEPENDENT — offset-ul unuia nu afectează offsetul celuilalt.
"""

import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
SINK_FILE = "output/pricing_alerts.jsonl"

ALERT_HIGH_EUR   = 120.0
ALERT_MEDIUM_EUR = 80.0

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "pricing-alerts",              
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}


def classify_price(price: float) -> str:
    if price > ALERT_HIGH_EUR:
        return "HIGH"
    elif price > ALERT_MEDIUM_EUR:
        return "MEDIUM"
    return "LOW"

def build_alert(zone: str, data: dict, partition: int, offset: int) -> dict:
    level = classify_price(data["spot_price_eur_mwh"])
    return {
        "alert_level":        level,
        "zone":               zone,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "spot_price_eur_mwh": data["spot_price_eur_mwh"],
        "consumption_mw":     data["consumption_mw"],
        "demand_level":       data["demand_level"],
        "kafka_partition":    partition,
        "kafka_offset":       offset,
    }


ICONS = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}

def run_consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(["energy.consumption"])

    print(f"[pricing-alerts] Consumer started | group.id='pricing-alerts'")
    print(f"[pricing-alerts] Thresholds: HIGH>{ALERT_HIGH_EUR} | MEDIUM>{ALERT_MEDIUM_EUR}")
    print(f"[pricing-alerts] Sink: {SINK_FILE}\n")

    with open(SINK_FILE, "a") as sink:
        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[pricing-alerts] ERROR: {msg.error()}")
                    continue

                zone = msg.key().decode("utf-8")
                data = json.loads(msg.value().decode("utf-8"))

                alert = build_alert(zone, data, msg.partition(), msg.offset())
                icon  = ICONS[alert["alert_level"]]

                print(f"[pricing-alerts] {icon} [{zone}] "
                      f"{alert['spot_price_eur_mwh']:.1f}€/MWh "
                      f"→ {alert['alert_level']} | "
                      f"partition={msg.partition()} offset={msg.offset()}")

                sink.write(json.dumps(alert) + "\n")
                sink.flush()

                # Print explicit pentru alertele importante
                if alert["alert_level"] in ("HIGH", "MEDIUM"):
                    print(f"  ⚡ PRICE ALERT: {zone} | "
                          f"consumption={data['consumption_mw']}MW | "
                          f"demand={data['demand_level']}")

        except KeyboardInterrupt:
            print("\n[pricing-alerts] Consumer stopped.")
        finally:
            consumer.close()

if __name__ == "__main__":
    run_consumer()
