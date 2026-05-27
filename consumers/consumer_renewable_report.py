"""
consumer_renewable_report.py
=============================
Consumer Group : renewable-report
Subscribed to  : energy.production
Produces to    : energy.alerts  (doar alerte reale)

Alerte produse în energy.alerts:
  - LOW_RENEWABLE_OUTPUT  → producție sub 15% din capacitate
  - WIND_DOMINANT         → vântul produce > 80% din total
  - SOLAR_DOMINANT        → solarul produce > 80% din total

Filtrare contextuală (alerte mai realiste):
  - LOW_RENEWABLE_OUTPUT noaptea         → NIGHT_EXPECTED, nu se trimite
  - LOW_RENEWABLE_OUTPUT pe ploaie/nori  → WEATHER_EXPECTED, nu se trimite
  - LOW_RENEWABLE_OUTPUT ziua pe senin   → REAL_ANOMALY, se trimite
  - SOLAR_DOMINANT noaptea               → imposibil fizic, se ignoră
  - WIND_DOMINANT                        → mereu relevant

Sink: output/renewable_report.jsonl
"""

import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

# ─── Config ───────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS  = "localhost:19092,localhost:19093,localhost:19094"
SOURCE_TOPIC       = "energy.production"
ALERTS_TOPIC       = "energy.alerts"
SINK_FILE          = "output/renewable_report.jsonl"

SOLAR_CAPACITY_MW  = 1000
WIND_CAPACITY_MW   = 1000
TOTAL_CAPACITY_MW  = SOLAR_CAPACITY_MW + WIND_CAPACITY_MW

LOW_PRODUCTION_PCT = 15.0
DOMINANT_PCT       = 80.0

DAY_START_HOUR     = 7
DAY_END_HOUR       = 21

OVERCAST_THRESHOLD = 80
PRECIPITATION_WEATHERCODES = {
    51, 53, 55,      # drizzle
    56, 57,          # freezing drizzle
    61, 63, 65,      # rain
    66, 67,          # freezing rain
    71, 73, 75, 77,  # snow
    80, 81, 82,      # rain showers
    85, 86,          # snow showers
    95, 96, 99,      # thunderstorm
}

# ─── Consumer / Producer config ───────────────────────────────────────────────

consumer_config = {
    "bootstrap.servers":       BOOTSTRAP_SERVERS,
    "group.id":                "renewable-report",
    "auto.offset.reset":       "latest",
    "enable.auto.commit":      True,
    "auto.commit.interval.ms": 5000,
}

alert_producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks":              "all",
    "retries":           5,
    "client.id":         "renewable-report-alerter",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def dominant_source(solar_mw: float, wind_mw: float) -> str:
    if solar_mw > wind_mw:
        return "solar"
    if wind_mw > solar_mw:
        return "wind"
    return "balanced"


def get_weather_context(data: dict) -> str:
    """
    Determină contextul meteo pentru a filtra alertele false.

    Returnează:
      NIGHT         → ora locală între 21:00 și 07:00
      PRECIPITATION → ploaie, ninsoare sau furtună activă
      OVERCAST      → acoperire nori > 80%
      NORMAL        → zi senină, orice alertă e o anomalie reală
    """
    hour        = datetime.now().hour
    cloudcover  = data.get("cloudcover") or 0
    weathercode = data.get("weathercode") or 0

    if hour >= DAY_END_HOUR or hour < DAY_START_HOUR:
        return "NIGHT"
    if weathercode in PRECIPITATION_WEATHERCODES:
        return "PRECIPITATION"
    if cloudcover > OVERCAST_THRESHOLD:
        return "OVERCAST"
    return "NORMAL"


def detect_alerts(
    total_capacity_pct: float,
    solar_mw: float,
    wind_mw: float,
    total_mw: float,
    weather_context: str,
) -> list[dict]:
    """
    Returnează lista alertelor care merită trimise în energy.alerts.
    Filtrează alertele false bazate pe context meteo și ora zilei.
    """
    alerts = []

    # LOW_RENEWABLE_OUTPUT — filtrare contextuală
    if total_capacity_pct < LOW_PRODUCTION_PCT:
        if weather_context == "NIGHT":
            pass  # normal noaptea, nu alertăm
        elif weather_context in ("PRECIPITATION", "OVERCAST"):
            pass  # normal pe vreme rea, nu alertăm
        else:
            # Ziua pe cer senin si productie mica → anomalie reală
            alerts.append({
                "alert_type": "LOW_RENEWABLE_OUTPUT",
                "reason":     "REAL_ANOMALY",
                "context":    weather_context,
            })

    # WIND_DOMINANT — mereu relevant, indiferent de context
    if total_mw > 0 and (wind_mw / total_mw * 100) > DOMINANT_PCT:
        alerts.append({
            "alert_type": "WIND_DOMINANT",
            "reason":     "WIND_SHARE_HIGH",
            "context":    weather_context,
        })

    # SOLAR_DOMINANT — doar ziua are sens fizic
    if weather_context == "NORMAL" and total_mw > 0:
        if (solar_mw / total_mw * 100) > DOMINANT_PCT:
            alerts.append({
                "alert_type": "SOLAR_DOMINANT",
                "reason":     "SOLAR_SHARE_HIGH",
                "context":    weather_context,
            })

    return alerts


def build_report(zone: str, data: dict, partition: int, offset: int) -> dict:
    solar_mw = float(data.get("solar_mw", 0))
    wind_mw  = float(data.get("wind_mw",  0))
    total_mw = float(data.get("total_renewable_mw", 0))

    solar_cf = round((solar_mw / SOLAR_CAPACITY_MW) * 100, 2)
    wind_cf  = round((wind_mw  / WIND_CAPACITY_MW)  * 100, 2)
    total_cf = round((total_mw / TOTAL_CAPACITY_MW) * 100, 2)

    weather_context = get_weather_context(data)
    alerts          = detect_alerts(total_cf, solar_mw, wind_mw, total_mw, weather_context)

    return {
        "country_zone":              zone,
        "city":                      data.get("city"),
        "timestamp":                 datetime.now(timezone.utc).isoformat(),
        "solar_mw":                  solar_mw,
        "wind_mw":                   wind_mw,
        "total_renewable_mw":        total_mw,
        "dominant_source":           dominant_source(solar_mw, wind_mw),
        "solar_capacity_factor_pct": solar_cf,
        "wind_capacity_factor_pct":  wind_cf,
        "total_capacity_factor_pct": total_cf,
        "weather_context":           weather_context,
        "weather_label":             data.get("weather_label", "UNKNOWN"),
        "cloudcover":                data.get("cloudcover"),
        "precipitation":             data.get("precipitation"),
        "weathercode":               data.get("weathercode"),
        "alerts":                    [a["alert_type"] for a in alerts],
        "kafka_partition":           partition,
        "kafka_offset":              offset,
    }


def build_alert_event(zone: str, alert: dict, report: dict) -> dict:
    return {
        "alert_type":                alert["alert_type"],
        "reason":                    alert["reason"],
        "weather_context":           alert["context"],
        "source_topic":              SOURCE_TOPIC,
        "zone":                      zone,
        "timestamp":                 report["timestamp"],
        "total_renewable_mw":        report["total_renewable_mw"],
        "total_capacity_factor_pct": report["total_capacity_factor_pct"],
        "dominant_source":           report["dominant_source"],
        "solar_mw":                  report["solar_mw"],
        "wind_mw":                   report["wind_mw"],
        "weather_label":             report["weather_label"],
        "cloudcover":                report["cloudcover"],
        "precipitation":             report["precipitation"],
    }


def alert_delivery_callback(err, msg):
    if err:
        print(f"  [ALERT ERROR] {err}")
    else:
        print(
            f"  [ALERT SENT] -> {ALERTS_TOPIC} "
            f"partition={msg.partition()} offset={msg.offset()} "
            f"key={msg.key().decode()}"
        )

# ─── Main ─────────────────────────────────────────────────────────────────────

def run_consumer():
    consumer       = Consumer(consumer_config)
    alert_producer = Producer(alert_producer_config)

    consumer.subscribe([SOURCE_TOPIC])

    print("[renewable-report] Consumer started")
    print(f"[renewable-report] group.id='renewable-report'")
    print(f"[renewable-report] Subscribed to : {SOURCE_TOPIC}")
    print(f"[renewable-report] Produces to   : {ALERTS_TOPIC} (doar alerte reale)")
    print(f"[renewable-report] Sink           : {SINK_FILE}")
    print(f"[renewable-report] Zi definita   : {DAY_START_HOUR}:00 - {DAY_END_HOUR}:00")
    print(f"[renewable-report] Filtrare      : NIGHT / PRECIPITATION / OVERCAST ignorate\n")

    with open(SINK_FILE, "a") as sink:
        try:
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[renewable-report] ERROR: {msg.error()}")
                    continue

                zone      = msg.key().decode("utf-8")
                data      = json.loads(msg.value().decode("utf-8"))
                partition = msg.partition()
                offset    = msg.offset()

                # 1. Procesează
                report = build_report(zone, data, partition, offset)

                # 2. Afișează în consolă
                status = "ALERT" if report["alerts"] else "OK"
                print(
                    f"[{status}] [{zone}] "
                    f"dominant={report['dominant_source']} | "
                    f"capacity={report['total_capacity_factor_pct']}% | "
                    f"context={report['weather_context']} | "
                    f"weather={report['weather_label']} | "
                    f"partition={partition} offset={offset}"
                )

                # 3. Scrie ÎNTOTDEAUNA în sink
                sink.write(json.dumps(report) + "\n")
                sink.flush()

                # 4. Produce în energy.alerts DOAR alertele reale
                alert_details = detect_alerts(
                    report["total_capacity_factor_pct"],
                    report["solar_mw"],
                    report["wind_mw"],
                    report["total_renewable_mw"],
                    report["weather_context"],
                )

                for alert in alert_details:
                    alert_event = build_alert_event(zone, alert, report)
                    alert_producer.produce(
                        topic=ALERTS_TOPIC,
                        key=zone,
                        value=json.dumps(alert_event),
                        callback=alert_delivery_callback,
                    )
                    print(
                        f"  [ALERT] [{alert['alert_type']}] "
                        f"reason={alert['reason']} context={alert['context']}"
                    )
                    alert_producer.poll(0)

                if alert_details:
                    alert_producer.flush()

        except KeyboardInterrupt:
            print("\n[renewable-report] Consumer oprit.")
            alert_producer.flush()
        finally:
            consumer.close()


if __name__ == "__main__":
    run_consumer()
