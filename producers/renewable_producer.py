import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "energy.production"

ZONES = {
    "RO": {"city": "Bucharest", "latitude": 44.43, "longitude": 26.10},
    "DE-LU": {"city": "Berlin", "latitude": 52.52, "longitude": 13.41},
    "FR": {"city": "Paris", "latitude": 48.85, "longitude": 2.35},
    "NL": {"city": "Amsterdam", "latitude": 52.37, "longitude": 4.90},
}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered | topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


def fetch_open_meteo(latitude, longitude):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        "&current=temperature_2m,wind_speed_10m,shortwave_radiation"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()["current"]


def estimate_generation_mw(weather):
    wind_speed = weather.get("wind_speed_10m") or 0
    solar_radiation = weather.get("shortwave_radiation") or 0

    solar_mw = round(solar_radiation * 1.8, 2)
    wind_mw = round((wind_speed ** 2) * 3.5, 2)
    total_mw = round(solar_mw + wind_mw, 2)

    return solar_mw, wind_mw, total_mw


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
    })

    while True:
        for country_zone, zone in ZONES.items():
            try:
                weather = fetch_open_meteo(
                    zone["latitude"],
                    zone["longitude"]
                )

                solar_mw, wind_mw, total_mw = estimate_generation_mw(weather)

                event = {
                    "event_type": "renewable_production",
                    "country_zone": country_zone,
                    "city": zone["city"],
                    "temperature_c": weather.get("temperature_2m"),
                    "wind_speed_10m": weather.get("wind_speed_10m"),
                    "shortwave_radiation": weather.get("shortwave_radiation"),
                    "solar_mw": solar_mw,
                    "wind_mw": wind_mw,
                    "total_renewable_mw": total_mw,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

                producer.produce(
                    topic=TOPIC,
                    key=country_zone,
                    value=json.dumps(event),
                    callback=delivery_report
                )

                producer.poll(0)
                print(f"Sent: {event}")

            except Exception as error:
                print(f"Error for {country_zone}: {error}")

        producer.flush()
        time.sleep(10)


if __name__ == "__main__":
    main()