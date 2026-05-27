"""
consumer_green_score.py
========================
Consumer Group: green-score
Subscribed to: energy.production + energy.consumption

Purpose:
  Calculate real-time green score (0-100) for each zone.
  Score reflects how much energy comes from renewable sources.

Green Score Formula (0-100):
  50% - Renewable Share (% of consumption covered by renewable)
  30% - Capacity Factor (how well potential is utilized)
  15% - Weather Bonus (generation in adverse weather conditions)
  5%  - Price Efficiency (generated at low spot price)

  Scoring:
    90-100 - EXCELLENT (>80% renewable share + factor >60%)
    70-89  - VERY_GOOD (>60% renewable share + factor >40%)
    50-69  - GOOD      (>40% renewable share + factor >25%)
    30-49  - FAIR      (>20% renewable share + factor >10%)
    0-29   - POOR      (<20% renewable share)

Sink: output/green_score.jsonl (time-series per zone)
Consumer Group: green-score
auto.offset.reset: earliest (captures full history)
"""

import json
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

# ─── Config ──────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
SINK_FILE = "output/green_score.jsonl"

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "green-score",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}

# Buffer pentru a păstra ultima valoare per zonă din fiecare topic
latest = defaultdict(dict)  # {"RO": {"production": {...}, "consumption": {...}}}

# ─── Green Score Logic ───────────────────────────────────────────────────────

DRIZZLE_CODES = {51, 53, 55, 56, 57}
RAIN_CODES = {61, 63, 65, 66, 67, 80, 81, 82}
SNOW_CODES = {71, 73, 75, 77, 85, 86}
THUNDERSTORM_CODES = {95, 96, 99}

def calculate_green_score(zone: str) -> dict | None:
    """
    Calculate green score (0-100) for a zone.
    Returns None if data from both topics is not available.
    """
    zone_data = latest[zone]
    if "production" not in zone_data or "consumption" not in zone_data:
        return None

    prod = zone_data["production"]
    cons = zone_data["consumption"]

    renewable_mw = float(prod.get("total_renewable_mw", 0))
    consumption_mw = float(cons.get("consumption_mw", 1))
    solar_mw = float(prod.get("solar_mw", 0))
    wind_mw = float(prod.get("wind_mw", 0))

    # Component 1: Renewable Share (50% of score)
    # What % of consumption is supplied by renewable
    renewable_share_pct = min(100, (renewable_mw / consumption_mw) * 100)

    if renewable_share_pct >= 80:
        renewable_score = 50  # Perfect
    elif renewable_share_pct >= 60:
        renewable_score = 45
    elif renewable_share_pct >= 40:
        renewable_score = 35
    elif renewable_share_pct >= 20:
        renewable_score = 20
    else:
        renewable_score = max(0, renewable_share_pct * 0.4)

    # Component 2: Capacity Factor (30% of score)
    # Efficient utilization of available capacity
    # Assumed capacity: Solar 1000 MW, Wind 1000 MW
    SOLAR_CAP = 1000
    WIND_CAP = 1000
    TOTAL_CAP = SOLAR_CAP + WIND_CAP

    total_capacity_factor = (renewable_mw / TOTAL_CAP) * 100

    if total_capacity_factor >= 60:
        cf_score = 30  # Excelent
    elif total_capacity_factor >= 40:
        cf_score = 25
    elif total_capacity_factor >= 25:
        cf_score = 18
    elif total_capacity_factor >= 10:
        cf_score = 10
    else:
        cf_score = max(0, total_capacity_factor * 0.3)

    # Component 3: Weather Bonus (15% of score)
    # Bonus: generation in adverse weather conditions = more impressive
    weathercode = int(prod.get("weathercode", 0))
    cloudcover = int(prod.get("cloudcover", 0))

    weather_bonus = 0
    weather_condition = "CLEAR"

    # WMO codes from Open-Meteo: drizzle/rain/snow/showers/thunderstorm groups.
    if weathercode in DRIZZLE_CODES:
        weather_bonus = min(15, 6 + (renewable_mw / 120))
        weather_condition = "DRIZZLE"
    elif weathercode in RAIN_CODES:
        weather_bonus = min(15, 8 + (renewable_mw / 100))
        weather_condition = "RAIN"
    elif weathercode in THUNDERSTORM_CODES:
        weather_bonus = min(15, 10 + (renewable_mw / 100))
        weather_condition = "STORM"
    elif weathercode in SNOW_CODES:
        weather_bonus = min(15, 7 + (renewable_mw / 100))
        weather_condition = "SNOW"
    elif cloudcover > 80:  # dense clouds
        weather_bonus = min(15, 5 + (renewable_mw / 200))
        weather_condition = "OVERCAST"
    else:
        weather_bonus = 0
        weather_condition = "CLEAR"

    # Component 4: Price Efficiency (5% of score)
    # Bonus: generated in good weather at low price = economic efficiency
    spot_price = float(cons.get("spot_price_eur_mwh", 80))
    price_bonus = 0

    if spot_price < 50 and renewable_share_pct > 50:
        price_bonus = 5  # Renewable generated in good weather + low price
    elif spot_price < 80 and renewable_share_pct > 40:
        price_bonus = 3
    else:
        price_bonus = 0

    # Total Green Score
    total_score = renewable_score + cf_score + weather_bonus + price_bonus
    green_score = round(min(100, max(0, total_score)), 1)

    # Rating classification
    if green_score >= 90:
        rating = "EXCELLENT"
    elif green_score >= 70:
        rating = "VERY_GOOD"
    elif green_score >= 50:
        rating = "GOOD"
    elif green_score >= 30:
        rating = "FAIR"
    else:
        rating = "POOR"

    return {
        "zone": zone,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "green_score": green_score,
        "rating": rating,
        "renewable_share_pct": round(renewable_share_pct, 1),
        "renewable_score_component": renewable_score,
        "capacity_factor_pct": round(total_capacity_factor, 1),
        "capacity_score_component": cf_score,
        "weather_condition": weather_condition,
        "weather_bonus": weather_bonus,
        "price_eur_mwh": spot_price,
        "price_bonus": price_bonus,
        "renewable_mw": renewable_mw,
        "consumption_mw": consumption_mw,
        "solar_mw": solar_mw,
        "wind_mw": wind_mw,
        "surplus_deficit_mw": round(renewable_mw - consumption_mw, 1),
        "demand_level": cons.get("demand_level"),
    }


def format_summary(score_dict: dict) -> str:
    """Format score for console display."""
    s = score_dict
    return (
        f"  {s['zone']:<4} | Score={s['green_score']:>6.1f} ({s['rating']:<11}) | "
        f"Renewable={s['renewable_share_pct']:>5.1f}% | "
        f"CF={s['capacity_factor_pct']:>5.1f}% | "
        f"Price={s['price_eur_mwh']:>6.1f}EUR/MWh | "
        f"Weather={s['weather_condition']:<10} | "
        f"Renewable={s['renewable_mw']:>7.0f}MW vs Consumption={s['consumption_mw']:>7.0f}MW"
    )


def run_consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(["energy.production", "energy.consumption"])

    print("[green-score] Consumer started | group.id=green-score")
    print("[green-score] Subscribed to: energy.production, energy.consumption")
    print("[green-score] Sink: output/green_score.jsonl")
    print("[green-score] Green score calculation running...\n")

    with open(SINK_FILE, "a") as sink:
        try:
            message_count = 0
            while True:
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"[ERROR] {msg.error()}")
                        break

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                except Exception as e:
                    print(f"[PARSE ERROR] {e}")
                    continue

                # Determine topic and zone
                topic = msg.topic()
                zone = data.get("zone") or data.get("country_zone")

                if not zone:
                    continue

                # Update buffer per topic
                if topic == "energy.production":
                    latest[zone]["production"] = data
                elif topic == "energy.consumption":
                    latest[zone]["consumption"] = data

                # Calculate green score
                score = calculate_green_score(zone)
                if score is not None:
                    message_count += 1

                    # Write to sink
                    sink.write(json.dumps(score) + "\n")
                    sink.flush()

                    # Display in console (every 5 messages)
                    if message_count % 5 == 0:
                        print(format_summary(score))

        except KeyboardInterrupt:
            print("\n[green-score] Consumer interrupted")
        finally:
            consumer.close()


if __name__ == "__main__":
    run_consumer()
