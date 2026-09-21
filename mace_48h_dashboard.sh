#!/usr/bin/env bash
# ============================================================================
# MACE TURNAROUND v1 -- 48H DASHBOARD  (v1.1)
# v1.1 fixes: (a) auto-locates portfolio.db from this script's directory,
#   (b) works WITHOUT the sqlite3 CLI via python3 fallback (read-only),
#   (c) no more integer-expected crashes when DB is missing,
#   (d) new [10] crypto virtual ledger snapshot (BONK staged-exit check).
# Override DB manually if needed:  MACE_DB=/full/path/portfolio.db ./script
# If journal sections come back empty, re-run with: sudo bash <script>
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${MACE_DB:-}"
if [ -z "$DB" ]; then
  for CAND in "$SCRIPT_DIR/config/portfolio.db" \
              "$(find "$SCRIPT_DIR" -maxdepth 3 -name portfolio.db -type f 2>/dev/null | head -1)" \
              "/home/tony/dev/MACE-LOCAL/config/portfolio.db"; do
    if [ -n "$CAND" ] && [ -f "$CAND" ]; then DB="$CAND"; break; fi
  done
fi
DB_OK=0; [ -n "$DB" ] && [ -f "$DB" ] && DB_OK=1
REPO="$SCRIPT_DIR"
WIN="48 hours ago"
NO_DB="  (DB not found - export MACE_DB=/full/path/portfolio.db and re-run)"

Q() {  # pretty table query: sqlite3 CLI if installed, else python3 shim
  if [ "$DB_OK" != 1 ]; then echo "$NO_DB"; return; fi
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 -header -column "$DB" "$1"
  else
    python3 - "$DB" "$1" <<'PYEOF'
import sys, sqlite3
conn = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
cur = conn.execute(sys.argv[2])
cols = [d[0] for d in cur.description] if cur.description else []
rows = cur.fetchall()
if cols:
    data = [[("" if v is None else str(v)) for v in r] for r in rows]
    w = [len(c) for c in cols]
    for r in data:
        for i, v in enumerate(r):
            w[i] = max(w[i], len(v))
    print("  ".join(c.ljust(w[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * x for x in w))
    for r in data:
        print("  ".join(v.ljust(w[i]) for i, v in enumerate(r)))
conn.close()
PYEOF
  fi
}
S() {  # scalar query with the same fallback
  if [ "$DB_OK" != 1 ]; then echo ""; return; fi
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$DB" "$1"
  else
    python3 -c 'import sys,sqlite3
r = sqlite3.connect("file:"+sys.argv[1]+"?mode=ro", uri=True).execute(sys.argv[2]).fetchone()
print("" if (r is None or r[0] is None) else r[0])' "$DB" "$1"
  fi
}

echo "########################################################"
echo "#  MACE 48H DASHBOARD v1.1  --  $(date -u '+%Y-%m-%dT%H:%MZ')"
echo "########################################################"
if [ "$DB_OK" = 1 ]; then echo "#  DB: $DB"; else echo "#  !! DB NOT FOUND — DB sections will show n/a"; fi

echo; echo "=== [0] FLEET STATE (context) ==="
for U in mace-tradfi-shield mace-crypto-shield mace-hwm-updater \
         mace-equities-orchestrator mace-crypto-orchestrator \
         mace-tradfi-news-guard; do
  printf "  %-30s %s\n" "$U" "$(systemctl is-active "$U" 2>/dev/null)"
done
git -C "$REPO" log -1 --format='  deploy HEAD: %h %s (%ci)' 2>/dev/null

echo; echo "=== [1] STOP-OUT PRESSURE  (target: 0-2/day) ==="
for U in mace-tradfi-shield mace-crypto-shield; do
  echo "--- $U : liquidations per day ---"
  journalctl -u "$U" --since "$WIN" --no-pager -o short 2>/dev/null \
    | grep -E 'DISPATCHING LIQUIDATION|EXECUTION REFLEX' \
    | awk '{print $1, $2}' | sort | uniq -c | sed 's/^/  /'
done
TL=$(journalctl -u mace-tradfi-shield --since "$WIN" --no-pager 2>/dev/null | wc -l)
CL=$(journalctl -u mace-crypto-shield  --since "$WIN" --no-pager 2>/dev/null | wc -l)
echo "  raw journal lines scanned (tradfi / crypto): $TL / $CL"
echo "  (if both are 0, journal is unreadable -> re-run with sudo)"
TSO=$(journalctl -u mace-tradfi-shield --since "$WIN" --no-pager 2>/dev/null | grep -c 'DISPATCHING LIQUIDATION')
CSO=$(journalctl -u mace-crypto-shield  --since "$WIN" --no-pager 2>/dev/null | grep -c 'EXECUTION REFLEX')
CBR=$(journalctl -u mace-crypto-shield  --since "$WIN" --no-pager 2>/dev/null | grep -c 'TRAILING STOP-LOSS BREACHED')
ROS=$(journalctl -u mace-crypto-orchestrator --since "$WIN" --no-pager 2>/dev/null | grep -c 'RISK-OFF SELL')
echo "  48h totals: tradfi stop-outs=$TSO  crypto stop-outs=$CSO  (crypto breaches=$CBR)"
echo "  (context: crypto RISK-OFF regime sells=$ROS — not stop-outs, no cooldown expected)"

echo; echo "=== [2] COOLDOWN INTEGRITY  (target: 1:1 with stop-outs) ==="
echo "-- trade_cooldowns rows per day/reason, last 48h --"
Q "SELECT date(closed_at) AS day, reason, COUNT(*) AS n
    FROM trade_cooldowns
    WHERE datetime(closed_at) >= datetime('now','-48 hours')
    GROUP BY day, reason ORDER BY day;"
echo "-- active locks right now --"
Q "SELECT symbol, closed_at, cooldown_until
    FROM trade_cooldowns
    WHERE datetime(cooldown_until) > datetime('now')
    ORDER BY cooldown_until DESC LIMIT 15;"
TCD=$(S "SELECT COUNT(*) FROM trade_cooldowns
    WHERE datetime(closed_at) >= datetime('now','-48 hours');")
TCD=${TCD:-0}
echo "  48h cooldown rows = $TCD   vs  48h stop-outs = $((TSO+CSO))"

echo; echo "=== [3] NameError / SHIELD CRASHES  (target: 0) ==="
NE=0
for U in mace-tradfi-shield mace-crypto-shield \
         mace-tradfi-news-guard mace-equities-orchestrator; do
  N=$(journalctl -u "$U" --since "$WIN" --no-pager 2>/dev/null | grep -c 'NameError')
  echo "  $U: NameError count = $N"
  NE=$((NE+N))
done

echo; echo "=== [4] QUEUE HEALTH  (target: PENDING = 0) ==="
Q "SELECT status, COUNT(*) AS n FROM mcp_requested_trades GROUP BY status;"
echo "-- 48h slice by action --"
Q "SELECT action, status, COUNT(*) AS n
    FROM mcp_requested_trades
    WHERE datetime(updated_at) >= datetime('now','-48 hours')
    GROUP BY action, status;"
PEN=$(S "SELECT COUNT(*) FROM mcp_requested_trades WHERE status='PENDING';")
PEN=${PEN:-0}

echo; echo "=== [5] PROBATION GATE  (equities-orchestrator, last 48h) ==="
journalctl -u mace-equities-orchestrator --since "$WIN" --no-pager 2>/dev/null \
  | grep 'post-stopout 72h probation' | tail -5 | sed 's/^/  /'
PROB=$(journalctl -u mace-equities-orchestrator --since "$WIN" --no-pager 2>/dev/null \
  | grep -c 'post-stopout 72h probation')
echo "  probation rejections in 48h = $PROB  (>0 = gate is actively working)"

echo; echo "=== [6] STOP DISTANCES  (equities 5-12%, crypto 6-16%) ==="
Q "SELECT symbol, ROUND(high_water_mark,2) AS hwm, ROUND(loss_limit*100,2) AS stop_pct,
      CASE WHEN loss_limit*100 BETWEEN 5 AND 12 THEN 'ok' ELSE 'OUT-OF-BAND' END AS flag
    FROM equities_hwm ORDER BY stop_pct;"
Q "SELECT symbol, ROUND(high_water_mark,2) AS hwm, ROUND(loss_limit*100,2) AS stop_pct,
      CASE WHEN loss_limit*100 BETWEEN 6 AND 16 THEN 'ok' ELSE 'OUT-OF-BAND' END AS flag
    FROM crypto_hwm ORDER BY stop_pct;"

echo; echo "=== [7] LIVE-PRICE HOTFIX v1.1.1  (target: 0 failures) ==="
LFP=$(journalctl -u mace-crypto-orchestrator --since "$WIN" --no-pager 2>/dev/null \
  | grep -c 'Live fill price fetch failed')
echo "  'Live fill price fetch failed' in 48h = $LFP"
echo "  (0 = hotfix applied, or no crypto exits in window; >0 = fetch hotfix missing)"

echo; echo "=== [8] SELLS + HWM HYGIENE  (last 48h) ==="
echo "-- crypto RISK-OFF / sell lines --"
journalctl -u mace-crypto-orchestrator --since "$WIN" --no-pager 2>/dev/null \
  | grep -E 'RISK-OFF|SELL' | tail -5 | sed 's/^/  /'
echo "-- SELL rows in queue, 48h --"
Q "SELECT symbol, status, ROUND(amount_usd,0) AS usd, updated_at
    FROM mcp_requested_trades
    WHERE action='SELL' AND datetime(updated_at) >= datetime('now','-48 hours')
    ORDER BY updated_at DESC LIMIT 10;"
echo "-- HWM hygiene purges (fires only when orphans found) --"
journalctl -u mace-tradfi-shield --since "$WIN" --no-pager 2>/dev/null \
  | grep 'HWM hygiene' | tail -3 | sed 's/^/  /'

echo; echo "=== [10] CRYPTO VIRTUAL LEDGER  (guardrail book of record) ==="
Q "SELECT blockchain, token, ROUND(quantity,4) AS qty,
      ROUND(avg_entry_price,6) AS avg_entry, ROUND(quantity*avg_entry_price,2) AS cost_usd
    FROM portfolio ORDER BY cost_usd DESC LIMIT 20;"
echo "  (RISK-OFF sells should drain token rows toward 0 and grow ARBITRUM/USDT cash)"

echo; echo "=== [9] VERDICT (48h) ==="
TOT=$((TSO+CSO))
PD=$(awk "BEGIN{printf \"%.1f\", $TOT/2}")
if   [ "$TOT" -le 4 ]; then echo "  stop-outs     : PASS  ($TOT in 48h = $PD/day; pre-patch ~16/day)"
elif [ "$TOT" -le 8 ]; then echo "  stop-outs     : WARN  ($TOT in 48h = $PD/day)"
else echo "  stop-outs     : FAIL  ($TOT in 48h = $PD/day)"; fi
if [ "$DB_OK" != 1 ]; then
  echo "  cooldown 1:1  : n/a   (DB not found)"
  echo "  PENDING queue : n/a   (DB not found)"
else
  if   [ "$TOT" -eq 0 ] && [ "$TCD" -eq 0 ]; then
    echo "  cooldown 1:1  : PASS  (0 stop-outs, 0 new cooldowns -- nothing to lock)"
  elif [ "$TOT" -eq 0 ]; then
    echo "  cooldown 1:1  : INFO  (0 stop-outs but $TCD cooldown rows -- check reasons)"
  elif [ "$TCD" -ge "$TOT" ]; then
    echo "  cooldown 1:1  : PASS  ($TCD cooldowns >= $TOT stop-outs)"
  else
    echo "  cooldown 1:1  : FAIL  ($TCD cooldowns < $TOT stop-outs -- registration leaking)"
  fi
  [ "$PEN" -eq 0 ] && echo "  PENDING queue : PASS  (0 jammed)" \
                    || echo "  PENDING queue : FAIL  ($PEN jammed)"
fi
[ "$NE"  -eq 0 ] && echo "  NameError     : PASS  (0 crashes)" \
                  || echo "  NameError     : FAIL  ($NE crashes)"
[ "$LFP" -eq 0 ] && echo "  live fills    : PASS  (0 fetch failures)" \
                  || echo "  live fills    : WARN  ($LFP failures -> apply v1.1.1 hotfix)"
echo
echo ">>> paste this whole output back to chat for interpretation."
