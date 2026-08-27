import os
import datetime
import time
import threading
import re
from flask import Flask, jsonify, render_template_string
import yfinance as yf
from pymongo import MongoClient
from zoneinfo import ZoneInfo

app = Flask(__name__)
scraper = cloudscraper.create_scraper()

# --- ENVIRONMENT VARIABLES & SECRETS ---
MONGO_URI = os.environ.get('MONGO_URI')
MYFX_EMAIL = os.environ.get('MYFX_EMAIL')
MYFX_PASSWORD = os.environ.get('MYFX_PASSWORD')

# --- MONGO DATABASE CONFIGURATION ---
client = MongoClient(MONGO_URI if MONGO_URI else "mongodb://localhost:27017/")
db = client["macro_sentiment_db"]

baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]
chart_history_collection = db["session_chart_history"]

def get_ny_time():
    """Calculates current New York Time using automatic EDT/EST Daylight Saving Time transition"""
    return datetime.datetime.now(ZoneInfo("America/New_York"))

def load_db_document(collection, doc_id="state_doc"):
    try:
        doc = collection.find_one({"_id": doc_id})
        return doc if doc else {}
    except Exception as e:
        print(f"Database Read Error: {str(e)}")
        return {}

def save_db_document(collection, data, doc_id="state_doc"):
    try:
        data["_id"] = doc_id
        collection.replace_one({"_id": doc_id}, data, upsert=True)
    except Exception as e:
        print(f"Database Write Error: {str(e)}")

def get_current_session_details(ny_dt):
    hour = ny_dt.hour
    if 3 <= hour < 8:
        return "LONDON", 3
    elif 8 <= hour < 18:
        return "NEW YORK", 8
    else:
        return "ASIA", 18

def clean_symbol_key(key_str):
    return re.sub(r'[^a-zA-Z]', '', str(key_str)).lower()

def fetch_live_data_from_api():
    try:
        login_url = f"https://www.myfxbook.com/api/login.json?email={MYFX_EMAIL}&password={MYFX_PASSWORD}"
        login_response = scraper.get(login_url, timeout=15).json()
        session_token = login_response.get("session")
        if not session_token:
            return None, None
            
        outlook_url = f"https://www.myfxbook.com/api/get-community-outlook.json?session={session_token}"
        raw_data = scraper.get(outlook_url, timeout=15).json()
        
        api_server_time = raw_data.get("timestamp", "")
        symbols_dict = {s['name']: s for s in raw_data.get("symbols", []) if 'name' in s}
        
        return symbols_dict, api_server_time
    except Exception as e:
        print(f"API Fetch Error Logged: {str(e)}")
        return None, None

# --- REAL-TIME INSTITUTIONAL ATR BREADTH ENGINE ---
def calculate_live_atr_breadth(pair_directions):
    tickers = {
        "EURGBP": "EURGBP=X",
        "AUDUSD": "AUDUSD=X",
        "NZDCHF": "NZDCHF=X",
        "CADJPY": "CADJPY=X"
    }
    
    total_atr_pips = 0.0
    total_consumed_pips = 0.0
    detailed_metrics = []
    
    ny_now = get_ny_time()
    if ny_now.hour >= 17:
        current_5pm_ny = ny_now.replace(hour=17, minute=0, second=0, microsecond=0)
    else:
        current_5pm_ny = (ny_now - datetime.timedelta(days=1)).replace(hour=17, minute=0, second=0, microsecond=0)
        
    for pair_name, target_dir in pair_directions.items():
        ticker_symbol = tickers.get(pair_name)
        if not ticker_symbol or target_dir == "NEUTRAL":
            continue
            
        try:
            t = yf.Ticker(ticker_symbol)
            intraday_data = t.history(period="2d", interval="15m")
            if intraday_data.empty:
                continue
                
            intraday_data.index = intraday_data.index.tz_convert('America/New_York')
            session_df = intraday_data[intraday_data.index >= current_5pm_ny]
            if session_df.empty:
                session_df = intraday_data.tail(1)
                
            current_price = float(session_df['Close'].iloc[-1])
            day_high = float(session_df['High'].max())
            day_low = float(session_df['Low'].min())
            
            hist = t.history(period="7d", interval="1d")
            if len(hist) >= 5:
                hist = hist.tail(6)
                true_ranges = []
                for i in range(1, len(hist)):
                    high = hist['High'].iloc[i]
                    low = hist['Low'].iloc[i]
                    prev_close = hist['Close'].iloc[i-1]
                    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                    true_ranges.append(tr)
                atr_5 = sum(true_ranges[-5:]) / 5.0
            else:
                atr_5 = (day_high - day_low) if (day_high > day_low) else 0.0100

            multiplier = 100.0 if "JPY" in pair_name else 10000.0
            
            if target_dir == "SELL":
                pips_consumed = (day_high - current_price) * multiplier
            else:
                pips_consumed = (current_price - day_low) * multiplier
                
            atr_pips = atr_5 * multiplier
            pips_consumed = max(0.0, pips_consumed)
            pips_remaining = max(0.0, atr_pips - pips_consumed)
            
            total_atr_pips += atr_pips
            total_consumed_pips += pips_consumed
            
            detailed_metrics.append({
                "pair": pair_name,
                "direction": target_dir,
                "current": round(current_price, 5),
                "high": round(day_high, 5),
                "low": round(day_low, 5),
                "atr": round(atr_pips, 1),
                "used": round(pips_consumed, 1),
                "left": round(pips_remaining, 1)
            })
        except Exception as ex:
            print(f"Intraday Data Error for {pair_name}: {str(ex)}")

    if total_atr_pips > 0:
        pct_left = round((max(0.0, total_atr_pips - total_consumed_pips) / total_atr_pips) * 100, 1)
    else:
        pct_left = 100.0
        
    return pct_left, detailed_metrics

# --- SESSION CHART SNAPSHOT ENGINE ---
def record_chart_snapshot(clean_live_pairs, current_session_label, current_date_str, ny_now):
    """Computes combined sentiment score snapshots and maintains history for WLS slope rendering."""
    try:
        stored_baseline = load_db_document(baseline_collection)
        baseline_volumes = stored_baseline.get("volumes", {})
        stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
        daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

        majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
        tracked_assets = majors + ["GOLD"]

        sess_long_delta, sess_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}
        daily_long_delta, daily_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}

        for name, live in clean_live_pairs.items():
            cleaned_name = clean_symbol_key(name)
            live_long = float(live.get("longVolume", 0) or 0)
            live_short = float(live.get("shortVolume", 0) or 0)

            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("longVolume") or live_long)
            b_short = float(base_marker.get("shortVolume") or live_short)

            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("longVolume") or live_long)
            d_short = float(daily_marker.get("shortVolume") or live_short)

            if cleaned_name == "xauusd":
                sess_long_delta["GOLD"] = (live_long - b_long)
                sess_short_delta["GOLD"] = (live_short - b_short)
                daily_long_delta["GOLD"] = (live_long - d_long)
                daily_short_delta["GOLD"] = (live_short - d_short)
                continue

            if len(cleaned_name) != 6: continue
            base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
            if base in majors and quote in majors:
                sess_long_delta[base] += (live_long - b_long)
                sess_short_delta[base] += (live_short - b_short)
                sess_long_delta[quote] += (live_short - b_short)
                sess_short_delta[quote] += (live_long - b_long)

                daily_long_delta[base] += (live_long - d_long)
                daily_short_delta[base] += (live_short - d_short)
                daily_long_delta[quote] += (live_short - d_short)
                daily_short_delta[quote] += (daily_long_delta.get(base, 0))

        stored_chart = load_db_document(chart_history_collection, "chart_state_doc")
        points = stored_chart.get("points", {}) if stored_chart.get("session") == current_session_label else {}

        timestamp = ny_now.strftime("%H:%M:%S")
        for cur in tracked_assets:
            net_shift = round((sess_long_delta[cur] - sess_short_delta[cur]) / 100.0, 2)
            d_net_shift = round((daily_long_delta[cur] - daily_short_delta[cur]) / 100.0, 2)
            combined_value = round(net_shift + d_net_shift, 2)
            points.setdefault(cur, []).append({"t": timestamp, "v": combined_value})
            points[cur] = points[cur][-600:]

        save_db_document(chart_history_collection, {
            "session": current_session_label,
            "session_date": current_date_str,
            "points": points
        }, "chart_state_doc")
    except Exception as e:
        print(f"Chart Snapshot Error: {str(e)}")

# --- BACKGROUND AUTOMATION ENGINE ---
def run_background_state_scheduler():
    print("Background Sentiment Automation Engine: Active.")
    while True:
        try:
            ny_now = get_ny_time()
            current_date_str = ny_now.strftime("%Y-%m-%d")
            current_session_label, session_anchor_hour = get_current_session_details(ny_now)
            
            session_start_dt = ny_now.replace(hour=session_anchor_hour, minute=0, second=0, microsecond=0)
            if current_session_label == "ASIA" and ny_now.hour < 18:
                session_start_dt = session_start_dt - datetime.timedelta(days=1)
                
            minutes_elapsed_in_session = (ny_now - session_start_dt).total_seconds() / 60.0
            is_active_trading_window = (minutes_elapsed_in_session >= 300.0) if current_session_label == "ASIA" else True

            cached_data = load_db_document(cache_collection)
            live_pairs = cached_data.get("live_pairs", {})
            last_api_fetch_str = cached_data.get("last_fetch_time", "")
            last_api_timestamp = cached_data.get("last_api_timestamp", "") 
            
            force_api_refresh = False
            if not live_pairs or not last_api_fetch_str:
                force_api_refresh = True
            elif is_active_trading_window:
                last_fetch_dt = datetime.datetime.strptime(last_api_fetch_str, "%Y-%m-%d %H:%M:%S")
                if (ny_now.replace(tzinfo=None) - last_fetch_dt).total_seconds() >= 780:
                    force_api_refresh = True

            if force_api_refresh:
                fresh_api_data, fresh_api_ts = fetch_live_data_from_api()
                if fresh_api_data:
                    live_pairs = fresh_api_data
                    last_api_timestamp = fresh_api_ts if fresh_api_ts else last_api_timestamp
                    save_db_document(cache_collection, {
                        "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "last_api_timestamp": last_api_timestamp,
                        "live_pairs": live_pairs
                    })
                    
            clean_live_pairs = {str(k): v for k, v in live_pairs.items()}

            # --- DUAL-TRACKING CALIBRATION LOGIC ---
            stored_baseline = load_db_document(baseline_collection)
            
            session_did_change = (
                not stored_baseline or 
                (stored_baseline.get("active_session") != current_session_label and stored_baseline.get("pending_session") != current_session_label) or
                (stored_baseline.get("baseline_date") != current_date_str and current_session_label == "ASIA" and stored_baseline.get("pending_session") != current_session_label)
            )

            if session_did_change and clean_live_pairs:
                fresh_volumes = {}
                for name, data in clean_live_pairs.items():
                    session_start_spot = float(data.get("price") or data.get("avgPrice") or 0)
                    fresh_volumes[name] = {
                        "longVolume": float(data.get("longVolume", 0) or 0),
                        "shortVolume": float(data.get("shortVolume", 0) or 0),
                        "avgPrice": session_start_spot,
                        "session_open_price": session_start_spot
                    }
                
                updated_baseline = dict(stored_baseline) if stored_baseline else {}
                updated_baseline.update({
                    "pending_session": current_session_label,
                    "pending_date": current_date_str,
                    "pending_anchor_hour": session_anchor_hour,
                    "pending_volumes": fresh_volumes,
                    "transition_counter": 1
                })
                save_db_document(baseline_collection, updated_baseline)
                stored_baseline = updated_baseline

            elif force_api_refresh and stored_baseline.get("pending_session") and clean_live_pairs:
                current_count = stored_baseline.get("transition_counter", 0) + 1
                if current_count >= 3:
                    stored_baseline = {
                        "baseline_date": stored_baseline.get("pending_date"),
                        "active_session": stored_baseline.get("pending_session"),
                        "anchor_hour": stored_baseline.get("pending_anchor_hour"),
                        "volumes": stored_baseline.get("pending_volumes")
                    }
                    save_db_document(baseline_collection, stored_baseline)
                else:
                    baseline_collection.update_one(
                        {"_id": "state_doc"}, 
                        {"$set": {"transition_counter": current_count}}
                    )

            # --- OVERALL DAILY SENTIMENT ANCHOR CALIBRATION ---
            stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
            current_daily_anchor_date = ny_now.strftime("%Y-%m-%d") if ny_now.hour >= 17 else (ny_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

            if (not stored_daily_baseline or stored_daily_baseline.get("daily_anchor_date") != current_daily_anchor_date) and clean_live_pairs:
                fresh_daily_volumes = {name: {"longVolume": float(data.get("longVolume", 0) or 0), "shortVolume": float(data.get("shortVolume", 0) or 0)} for name, data in clean_live_pairs.items()}
                save_db_document(daily_baseline_collection, {
                    "daily_anchor_date": current_daily_anchor_date,
                    "volumes": fresh_daily_volumes,
                    "captured_at": ny_now.strftime("%Y-%m-%d %H:%M:%S")
                }, "daily_state_doc")

            if clean_live_pairs:
                record_chart_snapshot(clean_live_pairs, current_session_label, current_date_str, ny_now)

        except Exception as e:
            print(f"Background Loop Error Catch: {str(e)}")
            
        time.sleep(60)

# --- INTRADAY EQUAL-WEIGHTED RISK ENGINE ---
def calculate_babypips_gauge_score():
    babypips_matrix = {
        "AUD/JPY": {"ticker": "AUDJPY=X", "inverse": False, "weight": 1},
        "BTC/USD": {"ticker": "BTC-USD", "inverse": False, "weight": 1},
        "Copper": {"ticker": "HG=F", "inverse": False, "weight": 1},
        "JPN225": {"ticker": "^N225", "inverse": False, "weight": 1},
        "NAS100": {"ticker": "^NDX", "inverse": False, "weight": 1},
        "SPX500": {"ticker": "^GSPC", "inverse": False, "weight": 1},
        "USD Index": {"ticker": "DX-Y.NYB", "inverse": True, "weight": 1},
        "VOLX (VIX)": {"ticker": "^VIX", "inverse": True, "weight": 1},
        "XAU/USD": {"ticker": "GC=F", "inverse": True, "weight": 1}
    }
    
    total_weighted_score = 0.0
    total_weight = 0.0
    
    for name, config in babypips_matrix.items():
        try:
            ticker = yf.Ticker(config["ticker"])
            hist = ticker.history(period="30d")
            
            if len(hist) >= 2:
                close_prices = hist['Close']
                pct_changes = close_prices.pct_change().dropna() * 100
                daily_pct = pct_changes.iloc[-1]
                daily_std = pct_changes.std() if pct_changes.std() != 0 else 1.0
                
                asset_score = max(0.0, min(100.0, 50.0 + (daily_pct / (2.0 * daily_std)) * 50.0))
                if config["inverse"]:
                    asset_score = 100.0 - asset_score
                
                total_weighted_score += (asset_score * config["weight"])
                total_weight += config["weight"]
        except Exception as e:
            print(f"yfinance calculation error for {name}: {e}")
            
    if total_weight == 0:
        return 50.0, "NEUTRAL CONDITIONS", "#1e293b", "#38bdf8"
        
    final_gauge_value = round(total_weighted_score / total_weight, 1)
    if final_gauge_value >= 65.0:
        return final_gauge_value, "RISK ON REGIME", "#10b981", "#070a0f"
    elif final_gauge_value <= 35.0:
        return final_gauge_value, "RISK OFF REGIME", "#f43f5e", "#070a0f"
    else:
        return final_gauge_value, "NEUTRAL CONDITIONS", "#1e293b", "#38bdf8"

def process_sentiment_matrix():
    ny_now = get_ny_time()
    
    cached_data = load_db_document(cache_collection)
    live_pairs = cached_data.get("live_pairs", {})
    last_api_fetch_str = cached_data.get("last_fetch_time", "")
    sanitized_live_pairs = {str(k): v for k, v in live_pairs.items()}

    stored_baseline = load_db_document(baseline_collection)
    baseline_volumes = stored_baseline.get("volumes", {})
    active_session_label = stored_baseline.get("active_session", "INITIALIZING")

    stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
    daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

    majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
    tracked_assets = majors + ["GOLD"]
    
    absolute_long_pct_sum = {asset: 0.0 for asset in tracked_assets}
    absolute_pair_counts = {asset: 0 for asset in tracked_assets}
    session_long_delta, session_short_delta = {asset: 0.0 for asset in tracked_assets}, {asset: 0.0 for asset in tracked_assets}
    daily_long_delta, daily_short_delta = {asset: 0.0 for asset in tracked_assets}, {asset: 0.0 for asset in tracked_assets}
    
    for name, live in sanitized_live_pairs.items():
        cleaned_name = clean_symbol_key(name)
        live_long = float(live.get("longVolume", 0) or 0)
        live_short = float(live.get("shortVolume", 0) or 0)
        total_live = live_long + live_short

        if cleaned_name == "xauusd":
            if total_live > 0:
                absolute_long_pct_sum["GOLD"] = (live_long / total_live)
                absolute_pair_counts["GOLD"] = 1
                
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("longVolume") or live_long)
            b_short = float(base_marker.get("shortVolume") or live_short)
            session_long_delta["GOLD"] = (live_long - b_long)
            session_short_delta["GOLD"] = (live_short - b_short)
            
            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("longVolume") or live_long)
            d_short = float(daily_marker.get("shortVolume") or live_short)
            daily_long_delta["GOLD"] = (live_long - d_long)
            daily_short_delta["GOLD"] = (live_short - d_short)
            continue
            
        if len(cleaned_name) != 6: continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()
        
        if base in majors and quote in majors:
            if total_live > 0:
                absolute_long_pct_sum[base] += (live_long / total_live)
                absolute_pair_counts[base] += 1
                absolute_long_pct_sum[quote] += (live_short / total_live)
                absolute_pair_counts[quote] += 1
            
            base_marker = baseline_volumes.get(name, {})
            b_long = float(base_marker.get("longVolume") or live_long)
            b_short = float(base_marker.get("shortVolume") or live_short)
            session_long_delta[base] += (live_long - b_long)
            session_short_delta[base] += (live_short - b_short)
            session_long_delta[quote] += (live_short - b_short)
            session_short_delta[quote] += (live_long - b_long)

            daily_marker = daily_baseline_volumes.get(name, {})
            d_long = float(daily_marker.get("longVolume") or live_long)
            d_short = float(daily_marker.get("shortVolume") or live_short)
            daily_long_delta[base] += (live_long - d_long)
            daily_short_delta[base] += (live_short - d_short)
            daily_long_delta[quote] += (live_short - d_short)
            daily_short_delta[quote] += (daily_long_delta.get(base, 0))

    # --- FEATURE 1: POSITIONING DELTA VELOCITY AND ACCELERATION ENGINE (\Delta' / \Delta'') ---
    stored_chart = load_db_document(chart_history_collection, "chart_state_doc")
    chart_points = stored_chart.get("points", {}) if stored_chart else {}
    
    positioning_derivatives = {}
    for cur in tracked_assets:
        pts = chart_points.get(cur, [])
        velocity = 0.0      # 1st Derivative (\Delta')
        acceleration = 0.0  # 2nd Derivative (\Delta'')
        
        if len(pts) >= 2:
            velocity = pts[-1]["v"] - pts[-2]["v"]
        if len(pts) >= 3:
            prev_velocity = pts[-2]["v"] - pts[-3]["v"]
            acceleration = velocity - prev_velocity
            
        positioning_derivatives[cur] = {
            "velocity": round(velocity, 2),
            "acceleration": round(acceleration, 2)
        }

    currency_scores, daily_currency_scores = {}, {}
    for cur in tracked_assets:
        total_inv_count = absolute_pair_counts[cur]
        inv_long_ratio = (absolute_long_pct_sum[cur] / total_inv_count) if total_inv_count > 0 else 0.5
        display_name = "Gold" if cur == "GOLD" else cur

        formatted_score = round((session_long_delta[cur] - session_short_delta[cur]) / 100.0, 2)
        status_str = "UP" if formatted_score > 0 else ("DOWN" if formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        
        deriv = positioning_derivatives.get(cur, {"velocity": 0.0, "acceleration": 0.0})
        
        currency_scores[cur] = {
            "currency": display_name, 
            "raw_score": formatted_score, 
            "value": abs(formatted_score), 
            "status": status_str,
            "velocity": deriv["velocity"],
            "acceleration": deriv["acceleration"]
        }

        d_formatted_score = round((daily_long_delta[cur] - daily_short_delta[cur]) / 100.0, 2)
        d_status_str = "UP" if d_formatted_score > 0 else ("DOWN" if d_formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        daily_currency_scores[cur] = {
            "currency": display_name, 
            "value": abs(d_formatted_score), 
            "status": d_status_str,
            "velocity": deriv["velocity"],
            "acceleration": deriv["acceleration"]
        }
    
    top_4_up = [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"]
    bottom_4_down = [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"]

    daily_top_4_up = [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"]
    daily_bottom_4_down = [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"]

    def evaluate_trade_direction(base_ccy, quote_ccy):
        b_data, q_data = currency_scores.get(base_ccy), currency_scores.get(quote_ccy)
        if not b_data or not q_data: return "NEUTRAL"
        if b_data["status"] == "UP" and q_data["status"] == "UP": return "SELL" if b_data["value"] > q_data["value"] else "BUY"
        if b_data["status"] == "DOWN" and q_data["status"] == "DOWN": return "BUY" if b_data["value"] > q_data["value"] else "SELL"
        if b_data["status"] == "UP" and q_data["status"] == "DOWN": return "SELL"
        if b_data["status"] == "DOWN" and q_data["status"] == "UP": return "BUY"
        return "NEUTRAL"

    pair_signals = {
        "EURGBP": evaluate_trade_direction("EUR", "GBP"),
        "AUDUSD": evaluate_trade_direction("AUD", "USD"),
        "NZDCHF": evaluate_trade_direction("NZD", "CHF"),
        "CADJPY": evaluate_trade_direction("CAD", "JPY")
    }

    gauge_val, babypips_label, risk_color, risk_text_color = calculate_babypips_gauge_score()
    breadth_left_pct, atr_pair_details = calculate_live_atr_breadth(pair_signals)

    bias_output = []
    for cur in tracked_assets:
        count = absolute_pair_counts[cur]
        long_pct = round((absolute_long_pct_sum[cur] / count) * 100, 1) if count > 0 else 50.0
        bias_output.append({"currency": "Gold" if cur == "GOLD" else cur, "long_pct": long_pct, "bias_label": "BULLISH" if long_pct >= 50.0 else "BEARISH"})
    bias_output = sorted(bias_output, key=lambda x: x['long_pct'], reverse=True)

    display_sync = last_api_fetch_str if last_api_fetch_str else ny_now.strftime("%Y-%m-%d %H:%M:%S")
    pending_label = stored_baseline.get("pending_session")
    buffer_status = f"Caching new session baseline for {pending_label} (Gathered blocks: {stored_baseline.get('transition_counter', 0)}/3)." if pending_label and pending_label != active_session_label else None

    return {
        "top_4_up": top_4_up, "bottom_4_down": bottom_4_down,
        "daily_top_4_up": daily_top_4_up, "daily_bottom_4_down": daily_bottom_4_down,
        "absolute_bias": bias_output, "ny_time": ny_now.strftime("%I:%M:%S %p"),
        "api_sync_time": display_sync, "active_session": active_session_label,
        "baseline_set_at": f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)",
        "risk_regime": f"{babypips_label} ({gauge_val}%)", "risk_color": risk_color, "risk_text_color": risk_text_color,
        "signals": pair_signals, "breadth_left": breadth_left_pct, "atr_details": atr_pair_details, "buffer_status": buffer_status
    }

# --- DASHBOARD HTML CONTAINER ---
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Macro Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        h1 { margin: 0; font-size: 22px; color: #38bdf8; font-weight: 700; }
        h2 { font-size: 13px; color: #94a3b8; margin-top: 0; margin-bottom: 15px; text-transform: uppercase; letter-spacing: 0.5px;}
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; font-size: 12px; font-weight: 700; background-color: #111827; border: 1px solid #1f2937; color: #475569; text-transform: uppercase; letter-spacing: 1px; }
        .session-card.active-session-live { background-color: #1e1b4b; border: 2px solid #6366f1; color: #818cf8; box-shadow: 0 0 12px rgba(99, 102, 241, 0.3); }
        .regime-panel { background: linear-gradient(135deg, #111827 0%, #0f172a 100%); border: 1px solid #1e293b; border-radius: 12px; padding: 22px; margin-bottom: 25px; display: flex; justify-content: center; align-items: center; gap: 20px; }
        .regime-badge { padding: 12px 24px; border-radius: 8px; font-size: 16px; font-weight: 900; text-transform: uppercase; letter-spacing: 1px; min-width: 320px; text-align: center; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
        .breadth-box { text-align: right; background-color: #111827; border: 1px solid #1f2937; padding: 15px 25px; border-radius: 10px; min-width: 200px; }
        .breadth-val { font-size: 28px; font-weight: 900; color: #38bdf8; }
        .section-split { display: flex; flex-direction: row; gap: 25px; margin-bottom: 25px; width: 100%; align-items: flex-start; }
        .panel { background-color: #111827; border: 1px solid #1f2937; border-radius: 10px; padding: 20px; box-sizing: border-box; }
        .section-split > .panel { flex: 1; }
        .right-column-stack { flex: 1; display: flex; flex-direction: column; gap: 25px; }
        .velocity-row-container { display: flex; flex-direction: column; gap: 15px; margin-top: 10px; }
        .velocity-sub-heading { font-size: 11px; color: #64748b; text-transform: uppercase; font-weight: 700; margin-bottom: -5px; letter-spacing: 0.5px; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .grid-box { background-color: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 15px; text-align: center; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 6px; }
        .border-up { border-top: 3px solid #10b981; }
        .border-down { border-top: 3px solid #ef4444; }
        .currency-txt { font-size: 16px; font-weight: 700; color: #f1f5f9; }
        .value-box { font-size: 18px; font-weight: 800; }
        .deriv-metrics { font-size: 10px; color: #a5b4fc; font-family: monospace; }
        .up-color { color: #10b981; }
        .down-color { color: #ef4444; }
        .bias-list { display: flex; flex-direction: column; }
        .data-row { display: flex; align-items: center; padding: 11px 10px; border-bottom: 1px solid #1f2937; gap: 12px; }
        .bar-container { width: 110px; background-color: #334155; height: 8px; border-radius: 4px; overflow: hidden; }
        .bar-fill { height: 100%; }
        .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; width: 65px; text-align: center; }
        .badge-up { background-color: rgba(16, 185, 129, 0.15); color: #10b981; }
        .badge-down { background-color: rgba(239, 68, 68, 0.15); color: #ef4444; }
        .badge-bull { background-color: rgba(56, 189, 248, 0.15); color: #38bdf8; }
        .badge-bear { background-color: rgba(245, 158, 11, 0.15); color: #f59e0b; }
        .footer-note { background-color: #1e293b; padding: 12px 20px; border-radius: 8px; font-size: 12px; color: #94a3b8; border-left: 4px solid #38bdf8; margin-top: 25px; }
        .staging-alert { background-color: #0f172a; border: 1px dashed #38bdf8; border-radius: 6px; padding: 10px; margin-bottom: 20px; text-align: center; font-size: 12px; font-weight: 600; color: #38bdf8; }
        .audio-ctrl-btn { background-color: #ef4444; color: #fff; border: none; padding: 6px 14px; border-radius: 4px; font-size: 11px; font-weight: bold; cursor: pointer; text-transform: uppercase; letter-spacing: 0.5px; transition: all 0.2s ease; margin-left: 15px;}
        .audio-ctrl-btn.armed { background-color: #10b981; box-shadow: 0 0 8px rgba(16,185,129,0.4); }

        /* WLS Chart Grid Styles */
        .chart-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-top: 5px; }
        .chart-box { background-color: #1f2937; border-radius: 8px; padding: 12px; }
        .chart-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .chart-label { font-size: 13px; font-weight: 700; color: #e2e8f0; }
        .chart-canvas-wrap { position: relative; height: 95px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <div style="display: flex; align-items: center;">
                    <h1>Macro Sentiment Matrix Terminal</h1>
                    <button id="audioToggle" class="audio-ctrl-btn" onclick="toggleAudioSystem()">Audio System Muted</button>
                </div>
                <div style="color: #64748b; font-size: 12px; margin-top: 5px;">Active Anchor: <span style="color:#a5b4fc; font-weight:600;">{{ data.baseline_set_at }}</span></div>
            </div>
            <div class="timestamp" style="font-size: 12px; color: #64748b; text-align: right;">
                <div>Last API Fetch Sync: <span style="color: #38bdf8; font-weight:600;">{{ data.api_sync_time }}</span></div>
                <div style="color: #64748b; margin-top: 3px;">Local UI Heartbeat: {{ data.ny_time }} NY</div>
            </div>
        </div>

        {% if data.buffer_status %}
        <div class="staging-alert">
             ⚡ <strong>Continuous Data Stream Active:</strong> {{ data.buffer_status }}
        </div>
        {% endif %}

        <div class="session-tracker-bar">
            <div class="session-card {% if data.active_session == 'ASIA' %}active-session-live{% endif %}">Asia Session Open</div>
            <div class="session-card {% if data.active_session == 'LONDON' %}active-session-live{% endif %}">London Session Open</div>
            <div class="session-card {% if data.active_session == 'NEW YORK' %}active-session-live{% endif %}">New York Session Open</div>
        </div>

        <div class="regime-panel">
            {% if "NEUTRAL CONDITIONS" not in data.risk_regime %}
            <div class="breadth-box">
                <h2 style="margin: 0; font-size: 11px; color: #64748b;">Daily ATR Breadth Left</h2>
                <div class="breadth-val">{{ data.breadth_left }}%</div>
            </div>
            {% endif %}
            <div id="currentRegime" class="regime-badge" style="background-color: {{ data.risk_color }}; color: {{ data.risk_text_color }};">
                {{ data.risk_regime }}
            </div>
        </div>

        <div class="section-split">
            <div class="panel">
                <h2>Cumulative 24H Daily Sentiment Matrix (5:00 PM Anchor)</h2>
                <div class="velocity-row-container">
                    <div class="velocity-sub-heading">Daily Sentiment Up (Highest Value First)</div>
                    <div class="grid-row">
                        {% for item in data.daily_top_4_up %}
                        <div class="grid-box border-up">
                            <span class="currency-txt">{{ item.currency }}</span>
                            <span class="value-box up-color">{{ item.value }}</span>
                            <span class="deriv-metrics">Δ': {{ item.velocity }} | Δ'': {{ item.acceleration }}</span>
                            <span class="badge badge-up">{{ item.status }}</span>
                        </div>
                        {% endfor %}
                    </div>
                    
                    <div class="velocity-sub-heading" style="margin-top: 10px;">Daily Sentiment Down (Lowest Value First)</div>
                    <div class="grid-row">
                        {% for item in data.daily_bottom_4_down %}
                        <div class="grid-box border-down">
                            <span class="currency-txt">{{ item.currency }}</span>
                            <span class="value-box down-color">{{ item.value }}</span>
                            <span class="deriv-metrics">Δ': {{ item.velocity }} | Δ'': {{ item.acceleration }}</span>
                            <span class="badge badge-down">{{ item.status }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>

            <div class="right-column-stack">
                <div class="panel">
                    <h2>Active Session Value Shifts (Ranked Quantities)</h2>
                    <div class="velocity-row-container">
                        <div class="velocity-sub-heading">Sentiment Up (Highest Value First)</div>
                        <div class="grid-row">
                            {% for item in data.top_4_up %}
                            <div class="grid-box border-up">
                                <span class="currency-txt">{{ item.currency }}</span>
                                <span class="value-box up-color">{{ item.value }}</span>
                                <span class="deriv-metrics">Δ': {{ item.velocity }} | Δ'': {{ item.acceleration }}</span>
                                <span class="badge badge-up">{{ item.status }}</span>
                            </div>
                            {% endfor %}
                        </div>
                        
                        <div class="velocity-sub-heading" style="margin-top: 10px;">Sentiment Down (Lowest Value First)</div>
                        <div class="grid-row">
                            {% for item in data.bottom_4_down %}
                            <div class="grid-box border-down">
                                <span class="currency-txt">{{ item.currency }}</span>
                                <span class="value-box down-color">{{ item.value }}</span>
                                <span class="deriv-metrics">Δ': {{ item.velocity }} | Δ'': {{ item.acceleration }}</span>
                                <span class="badge badge-down">{{ item.status }}</span>
                            </div>
                            {% endfor %}
                        </div>
                    </div>
                </div>

                <div class="panel">
                    <h2>Absolute Retail Positioning Bias (Total Inventory)</h2>
                    <div class="bias-list">
                        {% for item in data.absolute_bias %}
                        <div class="data-row">
                            <span class="currency-txt" style="min-width: 50px;">{{ item.currency }}</span>
                            <span class="value-box" style="min-width: 50px; font-size: 14px; text-align: right; color: #f1f5f9; margin-right: 5px;">{{ item.long_pct }}%</span>
                            <div class="bar-container">
                                <div class="bar-fill" style="width: {{ item.long_pct }}%; background-color: {% if item.long_pct >= 50.0 %}#38bdf8{% else %}#f59e0b{% endif %};"></div>
                            </div>
                            <span class="badge {% if item.long_pct >= 50.0 %}badge-bull{% else %}badge-bear{% endif %}">{{ item.bias_label }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>
        </div>

        <!-- Session Sentiment WLS Trend Chart -->
        <div class="panel" style="margin-top: 25px;">
            <h2>Dual-Speed MTF WLS Slope (Fast λ=0.08, Slow λ=0.015, Dynamic R² Threshold)</h2>
            <div class="chart-grid">
                {% for cur in ['EUR','GBP','USD','AUD','NZD','CAD','CHF','JPY','GOLD'] %}
                <div class="chart-box">
                    <div class="chart-header">
                        <span class="chart-label">{{ 'Gold' if cur == 'GOLD' else cur }}</span>
                    </div>
                    <div class="chart-canvas-wrap"><canvas id="chart-{{ cur }}"></canvas></div>
                </div>
                {% endfor %}
            </div>
        </div>

        <div class="footer-note">
            <strong>Statistical Engine Notice:</strong> Regression lines utilize Dual-Speed WLS (Fast λ=0.08 & Slow λ=0.015). Solid Green/Red indicates confirmed dual-timeframe alignment meeting dynamic R² thresholds, while Light Green/Red indicates early unconfirmed micro-trend shifts.
        </div>
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
    <script>
        (function() {
            var breadthLeft = {{ data.breadth_left | tojson }};
            var atrUsedPct = 100 - (breadthLeft !== null ? breadthLeft : 0);

            // --- FEATURE 4: DYNAMIC VOLATILITY-ADJUSTED R^2 THRESHOLD CALCULATION ---
            var dynamicR2Cutoff = 0.35; // Default standard threshold
            if (atrUsedPct < 30) {
                dynamicR2Cutoff = 0.45; // Consolidation / Low Volatility Filter
            } else if (atrUsedPct > 60) {
                dynamicR2Cutoff = 0.25; // Active Expansion / High Volatility Surges Filter
            }

            /**
             * Advanced Weighted Least Squares (WLS) Regression Engine
             */
            function computeWlsRegression(values, lambda, targetR2) {
                if (!lambda) lambda = 0.03;
                var n = values.length;
                if (n < 5) {
                    return { fitted: values.slice(), isSignificant: false, slope: 0, r2: '0.00', tStat: '0.00' };
                }

                var sumW = 0, sumWX = 0, sumWY = 0, sumWXX = 0, sumWXY = 0;
                for (var i = 0; i < n; i++) {
                    var w = Math.exp(-lambda * (n - 1 - i));
                    sumW += w;
                    sumWX += w * i;
                    sumWY += w * values[i];
                    sumWXX += w * i * i;
                    sumWXY += w * i * values[i];
                }

                var denom = (sumW * sumWXX - sumWX * sumWX);
                var slope = denom !== 0 ? (sumW * sumWXY - sumWX * sumWY) / denom : 0;
                var intercept = (sumWY - slope * sumWX) / sumW;

                var yMean = sumWY / sumW;
                var xMean = sumWX / sumW;
                var ssTot = 0, ssRes = 0, sumWDiffX2 = 0;

                var fitted = [];
                for (var j = 0; j < n; j++) {
                    var yHat = slope * j + intercept;
                    fitted.push(yHat);
                    var w = Math.exp(-lambda * (n - 1 - j));
                    ssTot += w * Math.pow(values[j] - yMean, 2);
                    ssRes += w * Math.pow(values[j] - yHat, 2);
                    sumWDiffX2 += w * Math.pow(j - xMean, 2);
                }

                var r2 = ssTot > 0 ? Math.max(0, 1 - (ssRes / ssTot)) : 0;
                var df = Math.max(1, n - 2);
                var seSlope = (sumWDiffX2 > 0) ? Math.sqrt((ssRes / df) / sumWDiffX2) : 0.0001;
                var tStat = seSlope > 0 ? (slope / seSlope) : 0;

                var isSignificant = (r2 >= targetR2) && (Math.abs(tStat) > 1.96);

                return {
                    fitted: fitted,
                    isSignificant: isSignificant,
                    slope: slope,
                    r2: r2.toFixed(2),
                    tStat: tStat.toFixed(2)
                };
            }

            var chartHistory = {{ chart_history | tojson }};
            var currencies = ['EUR','GBP','USD','AUD','NZD','CAD','CHF','JPY','GOLD'];

            currencies.forEach(function(cur) {
                var points = chartHistory[cur] || [];
                var canvas = document.getElementById('chart-' + cur);
                if (!canvas) return;

                var values = points.map(function(p) { return p.v; });
                var labels = points.map(function(p) { return p.t; });

                // --- FEATURE 2: DUAL-SPEED MULTI-TIMEFRAME WLS ALIGNMENT ---
                var fastWls = computeWlsRegression(values, 0.08, dynamicR2Cutoff);  // Fast (~15m micro)
                var slowWls = computeWlsRegression(values, 0.015, dynamicR2Cutoff); // Slow (~1h session)

                // Alignment Check: Both Fast and Slow meet dynamic significance AND share the same directional sign
                var isDualAligned = fastWls.isSignificant && slowWls.isSignificant &&
                                   ((fastWls.slope > 0 && slowWls.slope > 0) || (fastWls.slope < 0 && slowWls.slope < 0));

                // Tiered 4-State Color Logic: Solid for high-conviction alignment, Soft/Light for early unconfirmed slopes
                var finalSlopeColor;
                if (fastWls.slope >= 0) {
                    // Upward Micro-Trend
                    finalSlopeColor = isDualAligned ? '#10b981' : 'rgba(16, 185, 129, 0.35)'; // Solid Green vs Light Green
                } else {
                    // Downward Micro-Trend
                    finalSlopeColor = isDualAligned ? '#ef4444' : 'rgba(239, 68, 68, 0.35)'; // Solid Red vs Light Red
                }

                new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                data: labels.map(function() { return 0; }),
                                borderColor: 'rgba(255, 255, 255, 0.2)',
                                borderWidth: 1,
                                borderDash: [4, 4],
                                pointRadius: 0,
                                fill: false,
                                tension: 0
                            },
                            {
                                data: fastWls.fitted,
                                borderColor: finalSlopeColor,
                                borderWidth: 2,
                                pointRadius: 0,
                                fill: false,
                                tension: 0
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { display: false },
                            y: { grid: { color: '#1f2937' }, ticks: { color: '#64748b', font: { size: 9 } } }
                        }
                    }
                });
            });
        })();
    </script>
    <script>
        function emitRegimeSoundSignal(type) {
            try {
                const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                if (type === "RISK_ON") {
                    triggerTone(audioCtx, 523.25, "sine", 0.0, 0.15); 
                    triggerTone(audioCtx, 659.25, "sine", 0.12, 0.3); 
                } else if (type === "RISK_OFF") {
                    triggerTone(audioCtx, 311.13, "sawtooth", 0.0, 0.2); 
                    triggerTone(audioCtx, 293.66, "sawtooth", 0.15, 0.35); 
                } else {
                    triggerTone(audioCtx, 440.00, "triangle", 0.0, 0.25); 
                }
            } catch (err) {
                console.error("Audio Context processing execution error:", err);
            }
        }

        function triggerTone(ctx, freq, waveType, startTime, duration) {
            const osc = ctx.createOscillator();
            const gainNode = ctx.createGain();
            osc.type = waveType;
            osc.frequency.setValueAtTime(freq, ctx.currentTime + startTime);
            gainNode.gain.setValueAtTime(0.15, ctx.currentTime + startTime);
            gainNode.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + startTime + duration);
            osc.connect(gainNode);
            gainNode.connect(ctx.destination);
            osc.start(ctx.currentTime + startTime);
            osc.stop(ctx.currentTime + startTime + duration);
        }

        function toggleAudioSystem() {
            const btn = document.getElementById("audioToggle");
            if (localStorage.getItem("regimeAudioArmed") === "true") {
                localStorage.setItem("regimeAudioArmed", "false");
                btn.textContent = "Audio System Muted";
                btn.classList.remove("armed");
            } else {
                localStorage.setItem("regimeAudioArmed", "true");
                btn.textContent = "Audio System Armed";
                btn.classList.add("armed");
                emitRegimeSoundSignal("NEUTRAL");
            }
        }

        document.addEventListener("DOMContentLoaded", function() {
            const btn = document.getElementById("audioToggle");
            const rawText = document.getElementById("currentRegime").textContent.trim();
            let cleanRegime = "NEUTRAL";
            if (rawText.includes("RISK ON")) cleanRegime = "RISK_ON";
            if (rawText.includes("RISK OFF")) cleanRegime = "RISK_OFF";

            if (localStorage.getItem("regimeAudioArmed") === "true") {
                btn.textContent = "Audio System Armed";
                btn.classList.add("armed");
                const previousKnownRegime = localStorage.getItem("lastSavedRegimeState");
                if (previousKnownRegime && previousKnownRegime !== cleanRegime) {
                    emitRegimeSoundSignal(cleanRegime);
                }
            }
            localStorage.setItem("lastSavedRegimeState", cleanRegime);
        });

        setInterval(function(){ location.reload(); }, 60000); 
    </script>
</body>
</html>
"""

bg_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
bg_thread.start()

@app.route('/')
def index():
    try:
        calculated_matrix = process_sentiment_matrix()
        chart_data = load_db_document(chart_history_collection, "chart_state_doc").get("points", {})
        return render_template_string(DASHBOARD_HTML, data=calculated_matrix, chart_history=chart_data)
    except Exception as e:
        return jsonify({"error": True, "message": f"Processing Runtime Failure: {str(e)}"}), 500

# --- AUTOMATED MT4 BRIDGE API ENDPOINT ---
@app.route('/api/mt4_signals')
def mt4_signals():
    try:
        matrix = process_sentiment_matrix()
        ny_now = get_ny_time()
        
        daily_raw = matrix.get('risk_regime', 'NEUTRAL CONDITIONS')
        daily_status = "RISK ON" if "RISK ON" in daily_raw else ("RISK OFF" if "RISK OFF" in daily_raw else "NEUTRAL")
            
        bias_list = matrix.get('absolute_bias', [])
        bias_lookup = {item['currency']: item['long_pct'] for item in bias_list}
        
        inv_on_matches, inv_off_matches = 0, 0
        eur_pct, gbp_pct = bias_lookup.get("EUR", 50.0), bias_lookup.get("GBP", 50.0)
        aud_pct, usd_pct = bias_lookup.get("AUD", 50.0), bias_lookup.get("USD", 50.0)
        nzd_pct, chf_pct = bias_lookup.get("NZD", 50.0), bias_lookup.get("CHF", 50.0)
        cad_pct, jpy_pct = bias_lookup.get("CAD", 50.0), bias_lookup.get("JPY", 50.0)
        
        if eur_pct > gbp_pct: inv_on_matches += 1
        else: inv_off_matches += 1
            
        if aud_pct > usd_pct: inv_off_matches += 1
        else: inv_on_matches += 1
            
        if nzd_pct > chf_pct: inv_off_matches += 1
        else: inv_on_matches += 1
            
        if cad_pct > jpy_pct: inv_off_matches += 1
        else: inv_on_matches += 1
            
        inv_status = "RISK ON" if inv_on_matches >= 3 else ("RISK OFF" if inv_off_matches >= 3 else "NEUTRAL")
            
        return f"{daily_status},{inv_status},{ny_now.hour}"
    except Exception as e:
        print(f"MT4 Bridge Processing Error: {str(e)}")
        return "ERROR,ERROR,0"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860)
