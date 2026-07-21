import os
import datetime
import time
import threading
import re
import requests
import concurrent.futures
from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient

app = Flask(__name__)

# --- ENVIRONMENT VARIABLES & SECRETS ---
MONGO_URI = os.environ.get('MONGO_URI')
IG_API_KEY = os.environ.get('IG_API_KEY')
IG_USERNAME = os.environ.get('IG_USERNAME')
IG_PASSWORD = os.environ.get('IG_PASSWORD')
IG_ACCOUNT_TYPE = os.environ.get('IG_ACCOUNT_TYPE', 'demo').lower()  # 'demo' or 'live'

IG_BASE_URL = "https://demo-api.ig.com/gateway/deal" if IG_ACCOUNT_TYPE == "demo" else "https://api.ig.com/gateway/deal"

# --- MONGO DATABASE CONFIGURATION ---
client = MongoClient(MONGO_URI if MONGO_URI else "mongodb://localhost:27017/")
db = client["ig_sentiment_db"]

baseline_collection = db["session_baselines"]
daily_baseline_collection = db["daily_baselines"]
cache_collection = db["api_cache"]

# IG "market IDs" for FX majors + gold. Gold confirmed via the API
# Companion as "GC" (not "GOLD", which returns a valid 200 but always
# 0.0/0.0 -- that marketId exists but carries no sentiment coverage).
IG_SYMBOL_MAP = {
    "EURUSD": "EURUSD", "GBPUSD": "GBPUSD", "AUDUSD": "AUDUSD",
    "NZDUSD": "NZDUSD", "USDCHF": "USDCHF", "USDCAD": "USDCAD",
    "USDJPY": "USDJPY", "XAUUSD": "GC"
}

# --- HELPER: SCHEMA-AGNOSTIC DATA RETRIEVAL ---
def get_safe_volume(data_dict, primary_key, secondary_key, fallback_value):
    """Retrieves volume data regardless of key naming schema."""
    val = data_dict.get(primary_key)
    if val is None:
        val = data_dict.get(secondary_key)
    return float(val) if val is not None else float(fallback_value)

# --- IG SESSION STATE ---
# IG session tokens (CST / X-SECURITY-TOKEN) are valid for ~6 hours and get
# extended automatically while in use, so we authenticate once and reuse the
# cached tokens across scheduler ticks rather than logging in every poll --
# repeated logins are wasteful and a good way to get an API key throttled.
_ig_session = {"cst": None, "xst": None, "authenticated_at": None}
_ig_session_lock = threading.Lock()

def ig_authenticate():
    """Logs into the IG REST API (v2 session) and caches the session tokens."""
    try:
        resp = requests.post(
            f"{IG_BASE_URL}/session",
            headers={
                "X-IG-API-KEY": IG_API_KEY,
                "Version": "2",
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json; charset=UTF-8"
            },
            json={"identifier": IG_USERNAME, "password": IG_PASSWORD},
            timeout=10
        )
        if resp.status_code == 200:
            with _ig_session_lock:
                _ig_session["cst"] = resp.headers.get("CST")
                _ig_session["xst"] = resp.headers.get("X-SECURITY-TOKEN")
                _ig_session["authenticated_at"] = datetime.datetime.now(datetime.timezone.utc)
            return True
        print(f"IG Auth Error: {resp.status_code} {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"IG Auth Exception: {str(e)}")
        return False

def ensure_ig_session():
    """Re-authenticates only if there's no cached session or it's getting old."""
    with _ig_session_lock:
        stale = (
            not _ig_session["cst"] or
            not _ig_session["authenticated_at"] or
            (datetime.datetime.now(datetime.timezone.utc) - _ig_session["authenticated_at"]).total_seconds() > 5 * 3600
        )
    if stale:
        return ig_authenticate()
    return True

def fetch_ig(pair_name, market_id, retry=True):
    if not ensure_ig_session():
        return pair_name, None
    try:
        with _ig_session_lock:
            headers = {
                "X-IG-API-KEY": IG_API_KEY,
                "CST": _ig_session["cst"],
                "X-SECURITY-TOKEN": _ig_session["xst"],
                "Version": "1",
                "Accept": "application/json; charset=UTF-8"
            }
        res = requests.get(f"{IG_BASE_URL}/clientsentiment/{market_id}", headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            return pair_name, {
                "long": float(data.get("longPositionPercentage", 0)),
                "short": float(data.get("shortPositionPercentage", 0))
            }
        if res.status_code == 401 and retry:
            # Token likely expired early -- force one re-auth and retry once.
            if ig_authenticate():
                return fetch_ig(pair_name, market_id, retry=False)
        print(f"IG Fetch Error for {pair_name}: {res.status_code} {res.text[:200]}")
        return pair_name, None
    except Exception as e:
        print(f"IG Fetch Exception for {pair_name}: {str(e)}")
        return pair_name, None

# --- THREADING ENGINE ---
background_engine_thread = None

@app.before_request
def ensure_background_engine_running():
    global background_engine_thread
    if background_engine_thread is None or not background_engine_thread.is_alive():
        background_engine_thread = threading.Thread(target=run_background_state_scheduler, daemon=True)
        background_engine_thread.start()

def get_ny_time():
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    ny_offset = datetime.timedelta(hours=-4)
    return utc_now + ny_offset

def load_db_document(collection, doc_id="state_doc"):
    try:
        doc = collection.find_one({"_id": doc_id})
        return doc if doc else {}
    except Exception as e:
        return {}

def save_db_document(collection, data, doc_id="state_doc"):
    try:
        data["_id"] = doc_id
        collection.replace_one({"_id": doc_id}, data, upsert=True)
    except Exception as e:
        print(f"Database Write Error: {str(e)}")

def get_current_session_details(ny_dt):
    hour = ny_dt.hour
    if 3 <= hour < 8: return "LONDON", 3
    elif 8 <= hour < 18: return "NEW YORK", 8
    else: return "ASIA", 18

def clean_symbol_key(key_str):
    return re.sub(r'[^a-zA-Z]', '', str(key_str)).lower()

# --- BACKGROUND AUTOMATION ENGINE ---
def run_background_state_scheduler():
    while True:
        try:
            ny_now = get_ny_time()
            current_date_str = ny_now.strftime("%Y-%m-%d")
            current_session_label, session_anchor_hour = get_current_session_details(ny_now)

            # Fetch Data. Kept to a modest worker count since IG's non-trading
            # endpoints are rate-limited per API key/account; 8 pairs at once
            # is well within normal limits, but avoid pushing this much higher.
            symbols = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
                futures = {exe.submit(fetch_ig, p, m): p for p, m in IG_SYMBOL_MAP.items()}
                for f in concurrent.futures.as_completed(futures):
                    p, res = f.result()
                    if res: symbols[p] = res

            if symbols:
                cache_collection.replace_one({"_id": "state_doc"}, {
                    "_id": "state_doc",
                    "last_fetch_time": ny_now.strftime("%Y-%m-%d %H:%M:%S"),
                    "live_pairs": symbols
                }, upsert=True)

            # --- SESSION BASELINE DUAL-TRACKING CALIBRATION ---
            stored_baseline = load_db_document(baseline_collection)

            session_did_change = (
                not stored_baseline or
                (stored_baseline.get("active_session") != current_session_label and
                 stored_baseline.get("pending_session") != current_session_label)
            )

            if session_did_change and symbols:
                updated_baseline = dict(stored_baseline) if stored_baseline else {}
                updated_baseline.update({
                    "pending_session": current_session_label,
                    "pending_date": current_date_str,
                    "pending_anchor_hour": session_anchor_hour,
                    "pending_volumes": symbols,
                    "transition_counter": 1
                })
                save_db_document(baseline_collection, updated_baseline)
                stored_baseline = updated_baseline
                print(f"Handoff Log: Staging baseline sequence initiated for {current_session_label}. Reading 1/3 secured.")

            elif stored_baseline.get("pending_session") and symbols:
                current_count = stored_baseline.get("transition_counter", 0) + 1
                print(f"Handoff Log: Incremented update block {current_count}/3 for {stored_baseline.get('pending_session')}")

                if current_count >= 3:
                    stored_baseline = {
                        "baseline_date": stored_baseline.get("pending_date"),
                        "active_session": stored_baseline.get("pending_session"),
                        "anchor_hour": stored_baseline.get("pending_anchor_hour"),
                        "volumes": stored_baseline.get("pending_volumes")
                    }
                    save_db_document(baseline_collection, stored_baseline)
                    print(f"Handoff Complete: Safely hot-swapped session view over to: {stored_baseline.get('active_session')}")
                else:
                    baseline_collection.update_one(
                        {"_id": "state_doc"},
                        {"$set": {"transition_counter": current_count}}
                    )

            # Daily Anchor Logic
            stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
            current_daily_anchor_date = ny_now.strftime("%Y-%m-%d") if ny_now.hour >= 17 else (ny_now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

            if not stored_daily_baseline or stored_daily_baseline.get("daily_anchor_date") != current_daily_anchor_date:
                if symbols:
                    save_db_document(daily_baseline_collection, {
                        "daily_anchor_date": current_daily_anchor_date,
                        "volumes": symbols,
                        "captured_at": ny_now.strftime("%Y-%m-%d %H:%M:%S")
                    }, "daily_state_doc")

        except Exception as e:
            print(f"Loop Error: {str(e)}")
        time.sleep(60)

def process_sentiment_matrix():
    SCALE_FACTOR = 1000

    ny_now = get_ny_time()
    cached_data = load_db_document(cache_collection)
    live_pairs = {str(k): v for k, v in cached_data.get("live_pairs", {}).items()}
    stored_baseline = load_db_document(baseline_collection)
    baseline_volumes = stored_baseline.get("volumes", {})
    stored_daily_baseline = load_db_document(daily_baseline_collection, "daily_state_doc")
    daily_baseline_volumes = stored_daily_baseline.get("volumes", {})

    majors = ["EUR", "GBP", "USD", "AUD", "NZD", "CAD", "CHF", "JPY"]
    tracked_assets = majors + ["GOLD"]

    abs_long_pct_sum = {asset: 0.0 for asset in tracked_assets}
    abs_pair_counts = {asset: 0 for asset in tracked_assets}
    sess_long_delta, sess_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}
    daily_long_delta, daily_short_delta = {a: 0.0 for a in tracked_assets}, {a: 0.0 for a in tracked_assets}

    for name, live in live_pairs.items():
        cleaned_name = clean_symbol_key(name)
        l_long, l_short = float(live.get("long", 0)), float(live.get("short", 0))
        total_live = l_long + l_short

        b_val = baseline_volumes.get(name, {})
        d_val = daily_baseline_volumes.get(name, {})

        b_long = get_safe_volume(b_val, "long", "longVolume", l_long)
        b_short = get_safe_volume(b_val, "short", "shortVolume", l_short)
        d_long = get_safe_volume(d_val, "long", "longVolume", l_long)
        d_short = get_safe_volume(d_val, "short", "shortVolume", l_short)

        if cleaned_name == "gold":
            if total_live > 0:
                abs_long_pct_sum["GOLD"] = (l_long / total_live)
                abs_pair_counts["GOLD"] = 1
            sess_long_delta["GOLD"] = (l_long - b_long)
            sess_short_delta["GOLD"] = (l_short - b_short)
            daily_long_delta["GOLD"] = (l_long - d_long)
            daily_short_delta["GOLD"] = (l_short - d_short)
            continue

        if len(cleaned_name) != 6: continue
        base, quote = cleaned_name[0:3].upper(), cleaned_name[3:6].upper()

        if base in majors and quote in majors:
            if total_live > 0:
                abs_long_pct_sum[base] += (l_long / total_live); abs_pair_counts[base] += 1
                abs_long_pct_sum[quote] += (l_short / total_live); abs_pair_counts[quote] += 1

            sess_long_delta[base] += (l_long - b_long); sess_short_delta[base] += (l_short - b_short)
            sess_long_delta[quote] += (l_short - b_short); sess_short_delta[quote] += (l_long - b_long)

            daily_long_delta[base] += (l_long - d_long); daily_short_delta[base] += (l_short - d_short)
            daily_long_delta[quote] += (l_short - d_short); daily_short_delta[quote] += (l_long - d_long)

    currency_scores, daily_currency_scores, bias_output = {}, {}, []

    for cur in tracked_assets:
        count = abs_pair_counts[cur]
        inv_long_ratio = (abs_long_pct_sum[cur] / count) if count > 0 else 0.5
        display_name = "Gold" if cur == "GOLD" else cur
        bias_output.append({"currency": display_name, "long_pct": round(inv_long_ratio * 100, 1), "bias_label": "BULLISH" if inv_long_ratio >= 0.5 else "BEARISH"})

        net_shift = (sess_long_delta[cur] - sess_short_delta[cur]) * SCALE_FACTOR
        formatted_score = int(round(net_shift, 0))
        status_str = "UP" if formatted_score > 0 else ("DOWN" if formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        currency_scores[cur] = {"currency": display_name, "value": abs(formatted_score), "status": status_str}

        d_net_shift = (daily_long_delta[cur] - daily_short_delta[cur]) * SCALE_FACTOR
        d_formatted_score = int(round(d_net_shift, 0))
        d_status_str = "UP" if d_formatted_score > 0 else ("DOWN" if d_formatted_score < 0 else ("UP" if inv_long_ratio >= 0.5 else "DOWN"))
        daily_currency_scores[cur] = {"currency": display_name, "value": abs(d_formatted_score), "status": d_status_str}

    return {
        "top_4_up": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "bottom_4_down": [x for x in sorted(list(currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "daily_top_4_up": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=True) if x['status'] == "UP"],
        "daily_bottom_4_down": [x for x in sorted(list(daily_currency_scores.values()), key=lambda x: x['value'], reverse=False) if x['status'] == "DOWN"],
        "absolute_bias": sorted(bias_output, key=lambda x: x['long_pct'], reverse=True),
        "ny_time": ny_now.strftime("%I:%M:%S %p"), "api_sync_time": cached_data.get("last_fetch_time", "Syncing..."),
        "active_session": get_current_session_details(ny_now)[0],
        "baseline_set_at": f"{stored_baseline.get('active_session')} Open ({stored_baseline.get('anchor_hour')}:00 NY)" if stored_baseline.get('active_session') else "Init"
    }

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IG Sentiment Matrix Terminal</title>
    <style>
        body { background-color: #0b0e14; color: #e2e8f0; font-family: -apple-system, sans-serif; margin: 0; padding: 25px; }
        .container { max-width: 1300px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 15px; margin-bottom: 25px; }
        h1 { margin: 0; font-size: 22px; color: #38bdf8; font-weight: 700; }
        h2 { font-size: 13px; color: #94a3b8; margin-bottom: 15px; text-transform: uppercase; }
        .session-tracker-bar { display: flex; gap: 10px; margin-bottom: 25px; }
        .session-card { flex: 1; padding: 12px; border-radius: 8px; text-align: center; font-size: 12px; font-weight: 700; background-color: #111827; border: 1px solid #1f2937; color: #475569; }
        .active-session-live { background-color: #1e1b4b; border: 2px solid #6366f1; color: #818cf8; }
        .section-split { display: flex; flex-direction: row; gap: 25px; width: 100%; align-items: flex-start; }
        .panel { background-color: #111827; border: 1px solid #1f1f23; border-radius: 10px; padding: 20px; flex: 1; }
        .right-column-stack { flex: 1; display: flex; flex-direction: column; gap: 25px; }
        .grid-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
        .grid-box { background-color: #1f2937; border-radius: 8px; padding: 15px; text-align: center; border-top: 3px solid #374151; }
        .border-up { border-top-color: #10b981; }
        .border-down { border-top-color: #ef4444; }
        .currency-txt { font-size: 16px; font-weight: 700; }
        .value-box { font-size: 18px; font-weight: 800; display: block; margin: 5px 0; }
        .badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }
        .bias-list { display: flex; flex-direction: column; }
        .data-row { display: flex; align-items: center; padding: 11px 10px; border-bottom: 1px solid #1f2937; gap: 12px; }
        .bar-container { width: 100px; background: #334155; height: 8px; border-radius: 4px; }
        .bar-fill { height: 100%; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div><h1>IG Sentiment Matrix Terminal</h1><div style="font-size: 12px; color: #64748b;">Active Anchor: {{ data.baseline_set_at }}</div></div>
            <div style="text-align: right; font-size: 12px; color: #64748b;">Sync: {{ data.api_sync_time }}<br>NY Time: {{ data.ny_time }}</div>
        </div>

        <div class="session-tracker-bar">
            <div class="session-card {{ 'active-session-live' if data.active_session == 'ASIA' else '' }}">ASIA SESSION OPEN</div>
            <div class="session-card {{ 'active-session-live' if data.active_session == 'LONDON' else '' }}">LONDON SESSION OPEN</div>
            <div class="session-card {{ 'active-session-live' if data.active_session == 'NEW YORK' else '' }}">NEW YORK SESSION OPEN</div>
        </div>

        <div class="section-split">
            <div class="panel">
                <h2>Cumulative 24H Daily Sentiment</h2>
                <div class="grid-row">{% for item in data.daily_top_4_up %}<div class="grid-box border-up"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #10b981;">{{ item.value }}</span><span class="badge" style="color:#10b981;">UP</span></div>{% endfor %}</div>
                <div class="grid-row" style="margin-top:10px;">{% for item in data.daily_bottom_4_down %}<div class="grid-box border-down"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #ef4444;">{{ item.value }}</span><span class="badge" style="color:#ef4444;">DOWN</span></div>{% endfor %}</div>
            </div>
            <div class="right-column-stack">
                <div class="panel">
                    <h2>Active Session Value Shifts</h2>
                    <div class="grid-row">{% for item in data.top_4_up %}<div class="grid-box border-up"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #10b981;">{{ item.value }}</span><span class="badge" style="color:#10b981;">UP</span></div>{% endfor %}</div>
                    <div class="grid-row" style="margin-top:10px;">{% for item in data.bottom_4_down %}<div class="grid-box border-down"><span class="currency-txt">{{ item.currency }}</span><span class="value-box" style="color: #ef4444;">{{ item.value }}</span><span class="badge" style="color:#ef4444;">DOWN</span></div>{% endfor %}</div>
                </div>
                <div class="panel">
                    <h2>Absolute Retail Positioning Bias (Client Count)</h2>
                    <div class="bias-list">
                        {% for item in data.absolute_bias %}
                        <div class="data-row">
                            <span class="currency-txt" style="min-width: 50px;">{{ item.currency }}</span>
                            <span style="font-size: 14px; color: #f1f5f9; margin-right: 10px; min-width: 45px; text-align: right;">{{ item.long_pct }}%</span>
                            <div class="bar-container"><div class="bar-fill" style="width: {{ item.long_pct }}%; background-color: {% if item.long_pct >= 50.0 %}#38bdf8{% else %}#f59e0b{% endif %};"></div></div>
                            <span class="badge" style="margin-left: 10px; background: rgba(56, 189, 248, 0.1); color: #38bdf8;">{{ item.bias_label }}</span>
                        </div>
                        {% endfor %}
                    </div>
                </div>
            </div>
        </div>
    </div>
    <script>setInterval(function(){ location.reload(); }, 60000);</script>
</body>
</html>
"""

@app.route('/')
def index():
    try: return render_template_string(DASHBOARD_HTML, data=process_sentiment_matrix())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 7860)))
