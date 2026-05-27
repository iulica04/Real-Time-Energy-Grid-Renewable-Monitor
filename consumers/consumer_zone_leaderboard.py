"""
consumer_zone_leaderboard.py
=============================
Consumer Group: zone-leaderboard
Subscribed to: energy.production

Purpose:
  Display live ranking of zones by renewable energy production.
  Simple but spectacular for demo - shows who's leading in real-time!

Ranking criteria:
  1. Total renewable MW (primary)
  2. Renewable share % (secondary)
  3. Capacity factor % (tertiary)

Output format:
  Leaderboard with visual ranking:
  1st GOLD   - Top producer
  2nd SILVER - Close second
  3rd BRONZE - Third place
  Rest       - Regular ranking

Sink: output/zone_leaderboard.jsonl (time-series ranking snapshots)
Consumer Group: zone-leaderboard
auto.offset.reset: latest (real-time only)
"""

import json
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError

# Config

BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
SINK_FILE = "output/zone_leaderboard.jsonl"
TOTAL_RENEWABLE_CAPACITY_MW = 2000
ALLOWED_ZONES = {"RO", "DE", "FR", "PL"}

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "zone-leaderboard",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
}

# Buffer to hold latest data per zone
latest_production = {}

MEDALS = {
    1: ("GOLD", "1st"),
    2: ("SILVER", "2nd"),
    3: ("BRONZE", "3rd"),
}

def get_capacity_factor_pct(data: dict, total_mw: float) -> float:
    if data.get("total_capacity_factor_pct") is not None:
        return min(100.0, float(data["total_capacity_factor_pct"]))
    return min(100.0, (total_mw / TOTAL_RENEWABLE_CAPACITY_MW) * 100)


def build_leaderboard():
    """
    Build ranking of zones by renewable production.
    Returns list of zones sorted by total renewable MW (descending).
    """
    if not latest_production:
        return []

    # Sort by total_renewable_mw (descending)
    sorted_zones = sorted(
        latest_production.items(),
        key=lambda x: float(x[1].get("total_renewable_mw", 0)),
        reverse=True
    )

    leaderboard = []
    for rank, (zone, data) in enumerate(sorted_zones, 1):
        total_mw = float(data.get("total_renewable_mw", 0))
        solar_mw = float(data.get("solar_mw", 0))
        wind_mw = float(data.get("wind_mw", 0))
        capacity_factor = get_capacity_factor_pct(data, total_mw)
        weather = data.get("weather_label", "UNKNOWN")

        medal_info = MEDALS.get(rank, (None, None))
        medal_name = medal_info[0]
        medal_place = medal_info[1]

        leaderboard_entry = {
            "rank": rank,
            "zone": zone,
            "total_renewable_mw": round(total_mw, 1),
            "solar_mw": round(solar_mw, 1),
            "wind_mw": round(wind_mw, 1),
            "capacity_factor_pct": round(capacity_factor, 1),
            "weather": weather,
            "medal": medal_name,
            "place": medal_place,
        }

        leaderboard.append(leaderboard_entry)

    return leaderboard


def format_leaderboard_display(leaderboard):
    """Format leaderboard for console display."""
    if not leaderboard:
        return "No data yet..."

    output = "\n" + "=" * 100 + "\n"
    output += "ZONE LEADERBOARD - Real-Time Renewable Production Ranking\n"
    output += "=" * 100 + "\n"

    for entry in leaderboard:
        rank = entry["rank"]
        zone = entry["zone"]
        total = entry["total_renewable_mw"]
        solar = entry["solar_mw"]
        wind = entry["wind_mw"]
        cf = entry["capacity_factor_pct"]
        weather = entry["weather"]
        medal = entry["medal"]
        place = entry["place"]

        if medal:
            prefix = f"[{medal}] {place}"
        else:
            prefix = f"#{rank}"

        output += (
            f"  {prefix:<15} {zone:<6} "
            f"| Total={total:>7.1f}MW "
            f"| Solar={solar:>6.1f}MW Wind={wind:>6.1f}MW "
            f"| CF={cf:>5.1f}% "
            f"| {weather:<10}\n"
        )

    output += "=" * 100 + "\n"
    return output


def run_consumer():
    consumer = Consumer(consumer_config)
    consumer.subscribe(["energy.production"])

    print("[zone-leaderboard] Consumer started | group.id=zone-leaderboard")
    print("[zone-leaderboard] Subscribed to: energy.production")
    print("[zone-leaderboard] Sink: output/zone_leaderboard.jsonl")
    print("[zone-leaderboard] Live zone ranking by renewable production...\n")

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

                # Extract zone and update buffer
                zone = data.get("country_zone")
                if not zone or zone not in ALLOWED_ZONES:
                    continue

                latest_production[zone] = data

                # Build and output leaderboard
                message_count += 1
                leaderboard = build_leaderboard()

                if leaderboard:
                    # Write to sink
                    leaderboard_record = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "leaderboard": leaderboard,
                    }
                    sink.write(json.dumps(leaderboard_record) + "\n")
                    sink.flush()

                    # Display in real-time (every message)
                    print(format_leaderboard_display(leaderboard))

        except KeyboardInterrupt:
            print("\n[zone-leaderboard] Consumer interrupted")
        finally:
            consumer.close()


if __name__ == "__main__":
    run_consumer()
