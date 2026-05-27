"""
producer_energy.py
===================
Producer - Producție Energie Regenerabilă
Sursă : Open-Meteo API (gratuit, fără cheie)
Topic : energy.production
Key   : country_zone (ex: "RO", "DE", "FR", "PL")

Câmpuri meteo adăugate față de versiunea anterioară:
  - cloudcover      → % acoperire nori (0-100)
  - precipitation   → mm ploaie în ultima oră
  - weathercode     → codul WMO al vremii (0=senin, 61/63/65=ploaie, 95/96/99=furtună)

Acestea sunt folosite de consumer pentru a determina dacă o alertă
LOW_RENEWABLE_OUTPUT e o anomalie reală sau comportament așteptat
(noapte, ploaie, nori).
"""

import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "energy.production"

ZONES = {
    "RO":   {"city": "Bucharest", "latitude": 44.43, "longitude": 26.10},
    "DE":   {"city": "Berlin",    "latitude": 52.52, "longitude": 13.41},
    "FR":   {"city": "Paris",     "latitude": 48.85, "longitude": 2.35},
    "PL":   {"city": "Warsaw",    "latitude": 52.23, "longitude": 21.01},
}

# Coduri WMO weather - grupate dupa tabelul Open-Meteo/WMO
# https://open-meteo.com/en/docs#weathervariables
WEATHERCODE_LABELS = {
    (0,):           "CLEAR",
    (1, 2, 3):      "PARTLY_CLOUDY",
    (45, 48):       "FOG",
    (51, 53, 55):   "DRIZZLE",
    (56, 57):       "FREEZING_DRIZZLE",
    (61, 63, 65):   "RAIN",
    (66, 67):       "FREEZING_RAIN",
    (71, 73, 75):   "SNOW",
    (77,):          "SNOW_GRAINS",
    (80, 81, 82):   "RAIN_SHOWERS",
    (85, 86):       "SNOW_SHOWERS",
    (95,):          "THUNDERSTORM",
    (96, 99):       "THUNDERSTORM_HAIL",
}

def weathercode_label(code: int) -> str:
    for codes, label in WEATHERCODE_LABELS.items():
        if code in codes:
            return label
    return "CLOUDY"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered | topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


def fetch_open_meteo(latitude: float, longitude: float) -> dict:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        "&current=temperature_2m,wind_speed_10m,shortwave_radiation"
        ",cloudcover,precipitation,weathercode"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()["current"]


def estimate_generation_mw(weather: dict) -> tuple[float, float, float]:
    wind_speed      = weather.get("wind_speed_10m") or 0
    solar_radiation = weather.get("shortwave_radiation") or 0

    solar_mw = round(solar_radiation * 1.8, 2)
    wind_mw  = round((wind_speed ** 2) * 3.5, 2)
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
                weather = fetch_open_meteo(zone["latitude"], zone["longitude"])

                solar_mw, wind_mw, total_mw = estimate_generation_mw(weather)

                weathercode = weather.get("weathercode", 0)

                event = {
                    "event_type":          "renewable_production",
                    "country_zone":        country_zone,
                    "city":                zone["city"],
                    "temperature_c":       weather.get("temperature_2m"),
                    "wind_speed_10m":      weather.get("wind_speed_10m"),
                    "shortwave_radiation": weather.get("shortwave_radiation"),
                    "cloudcover":          weather.get("cloudcover"),       # % nori
                    "precipitation":       weather.get("precipitation"),    # mm/h
                    "weathercode":         weathercode,                     # cod WMO
                    "weather_label":       weathercode_label(weathercode),  # ex: RAIN
                    "solar_mw":            solar_mw,
                    "wind_mw":             wind_mw,
                    "total_renewable_mw":  total_mw,
                    "timestamp":           datetime.now(timezone.utc).isoformat(),
                }

                producer.produce(
                    topic=TOPIC,
                    key=country_zone,
                    value=json.dumps(event),
                    callback=delivery_report,
                )

                producer.poll(0)
                print(
                    f"[SENT] {country_zone} | "
                    f"solar={solar_mw}MW wind={wind_mw}MW total={total_mw}MW | "
                    f"weather={event['weather_label']} cloud={event['cloudcover']}% "
                    f"precip={event['precipitation']}mm"
                )

            except Exception as error:
                print(f"Error for {country_zone}: {error}")

        producer.flush()
        time.sleep(10)


if __name__ == "__main__":
    main()
