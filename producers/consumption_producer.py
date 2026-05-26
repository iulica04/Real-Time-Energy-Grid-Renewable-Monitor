"""
Data source: Elecz API (prețuri spot reale ENTSO-E) + Open-Meteo (temperatură reală)
Topic:       energy.consumption
Key:         country_zone (ex: "RO", "DE", "FR", "PL")

Surse:
  - Prețuri spot (€/MWh): https://elecz.com/signal/spot?zone=RO  ← date reale ENTSO-E
  - Temperatură (°C):     https://api.open-meteo.com              ← pentru estimare consum
  - Consumul MW este estimat din temperatură (frig = consum mare)
"""

import json
import time
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "energy.consumption"

ZONES = {
    "RO": {
        "elecz_zone":    "RO",
        "latitude":      44.43,   
        "longitude":     26.10,
        "base_load_mw":  6500,
        "peak_load_mw":  8200,
    },
    "DE": {
        "elecz_zone":    "DE",
        "latitude":      52.52,   
        "longitude":     13.41,
        "base_load_mw":  52000,
        "peak_load_mw":  80000,
    },
    "FR": {
        "elecz_zone":    "FR",
        "latitude":      48.85,   
        "longitude":     2.35,
        "base_load_mw":  42000,
        "peak_load_mw":  68000,
    },
    "PL": {
        "elecz_zone":    "PL",
        "latitude":      52.23,   
        "longitude":     21.01,
        "base_load_mw":  18000,
        "peak_load_mw":  26000,
    },
}


producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 300,
    "linger.ms": 10,
    "compression.type": "gzip",
}


def fetch_spot_price(elecz_zone: str) -> float | None:
    try:
        url = f"https://elecz.com/signal/spot?zone={elecz_zone}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Elecz returnează prețul în EUR c/kWh → convertim în EUR/MWh (* 10)
        price_c_kwh = data.get("price")
        if price_c_kwh is not None:
            return round(float(price_c_kwh) * 10, 2)  # c/kWh → EUR/MWh
        return None
    except Exception as e:
        print(f"  [WARN] Elecz API error pentru {elecz_zone}: {e}")
        return None


def fetch_temperature(latitude: float, longitude: float) -> float | None:
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}"
            f"&longitude={longitude}"
            "&current=temperature_2m"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()["current"]["temperature_2m"]
    except Exception as e:
        print(f"  [WARN] Open-Meteo error: {e}")
        return None


def estimate_consumption(temperature: float, base_mw: float, peak_mw: float) -> float:
    if temperature < 0:
        factor = 0.95      
    elif temperature < 10:
        factor = 0.80      
    elif temperature < 20:
        factor = 0.60      
    elif temperature < 28:
        factor = 0.70     
    else:
        factor = 0.90      

    return round(base_mw + (peak_mw - base_mw) * factor, 1)



def fetch_real_data(zone: str, config: dict) -> dict:
 
    spot_price = fetch_spot_price(config["elecz_zone"])
    temperature = fetch_temperature(config["latitude"], config["longitude"])
    
    if temperature is not None:
        consumption_mw = estimate_consumption(
            temperature,
            config["base_load_mw"],
            config["peak_load_mw"]
        )
        demand_level = (
            "HIGH"   if consumption_mw > config["base_load_mw"] * 1.15 else
            "LOW"    if consumption_mw < config["base_load_mw"] * 0.75 else
            "NORMAL"
        )
    else:
       
        consumption_mw = config["base_load_mw"]
        demand_level = "UNKNOWN"
        temperature = None

   
    if spot_price is None:
        spot_price = 80.0
        price_source = "fallback"
    else:
        price_source = "elecz_real"

    return {
        "zone":               zone,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "consumption_mw":     consumption_mw,
        "spot_price_eur_mwh": spot_price,
        "temperature_c":      temperature,
        "demand_level":       demand_level,
        "source":             price_source,
    }


def delivery_callback(err, msg):
    if err:
        print(f"[DELIVERY ERROR] key={msg.key()} partition={msg.partition()} "
              f"error={err}")
    else:
        print(f"[DELIVERED]  topic={msg.topic()} "
              f"partition={msg.partition()} "
              f"offset={msg.offset()} "
              f"key={msg.key().decode()}")


def run_producer(interval_seconds: float = 30.0):
    producer = Producer(producer_config)
    print(f"Producer started → topic '{TOPIC}' | interval={interval_seconds}s")
    print(f"Data sources: Elecz API (spot prices) + Open-Meteo (temperature)\n")

    try:
        while True:
            for zone, config in ZONES.items():
                print(f"  Fetching data for {zone}...")
                data = fetch_real_data(zone, config)

                producer.produce(
                    topic=TOPIC,
                    key=zone,
                    value=json.dumps(data),
                    callback=delivery_callback,
                )
                print(f"[SENT] {zone} | "
                      f"consumption={data['consumption_mw']}MW | "
                      f"price={data['spot_price_eur_mwh']}€/MWh | "
                      f"temp={data['temperature_c']}°C | "
                      f"demand={data['demand_level']} | "
                      f"source={data['source']}")

            producer.poll(0)
            producer.flush()
            print(f"\n--- Batch flushed @ {datetime.now().strftime('%H:%M:%S')} ---\n")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nProducer stopped.")
        producer.flush()

if __name__ == "__main__":
    run_producer(interval_seconds=30.0)