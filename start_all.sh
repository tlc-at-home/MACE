#!/usr/bin/env bash

# Set working directory to the directory containing this script to resolve relative paths
cd "$(dirname "$0")" || exit 1

echo "=================================================================="
echo "   M.A.C.E. (Momentum Autonomous Cognitive Engine) Control System "
echo "=================================================================="

# 1. Terminate any stale or duplicate instances to prevent conflicts and memory leaks
echo "[*] Auditing and terminating existing M.A.C.E. processes..."
pkill -f "crypto_shield.py" 2>/dev/null
pkill -f "tradfi_shield.py" 2>/dev/null
pkill -f "crypto/swarm/orchestrator.py" 2>/dev/null
pkill -f "equities/swarm/orchestrator.py" 2>/dev/null
sleep 1.5

# Parse command-line arguments (e.g. check for --sandbox flag)
SANDBOX_FLAG=""
for arg in "$@"; do
  if [ "$arg" = "--sandbox" ]; then
    SANDBOX_FLAG="--sandbox"
    echo "[!] Sandbox mode enabled for KuCoin Crypto Swarm."
  fi
done

# 2. Boot background daemons
echo "[*] Launching Crypto Shield (Stop-Loss reflex, 15m interval)..."
nohup python3.11 -u crypto/crypto_shield.py --daemon --interval 900 >> crypto_shield.log 2>&1 &
PID1=$!

echo "[*] Launching TradFi Shield (Stop-Loss reflex, 15m interval)..."
nohup python3.11 -u equities/tradfi_shield.py --daemon --interval 900 >> tradfi_shield.log 2>&1 &
PID2=$!

echo "[*] Launching Crypto Swarm Orchestrator (Alphanumeric Kelly, 4h interval)..."
nohup python3.11 -u crypto/swarm/orchestrator.py --daemon --interval 14400 $SANDBOX_FLAG >> crypto_daemon.log 2>&1 &
PID3=$!

echo "[*] Launching Equities Swarm Orchestrator (ML Regime Kelly, 1h interval)..."
nohup python3.11 -u equities/swarm/orchestrator.py --daemon --interval 3600 >> equities_daemon.log 2>&1 &
PID4=$!

echo "=================================================================="
echo "[+] All M.A.C.E. processes successfully initialized and running:"
echo "    - 🛡️  Crypto Shield PID:          $PID1 (Log: crypto_shield.log)"
echo "    - 🛡️  TradFi Shield PID:          $PID2 (Log: tradfi_shield.log)"
echo "    - ⚔️  Crypto Swarm PID:           $PID3 (Log: crypto_daemon.log)"
echo "    - ⚔️  Equities Swarm PID:         $PID4 (Log: equities_daemon.log)"
echo "=================================================================="
echo "[*] Monitoring is live. Use: 'tail -f *.log' to view current telemetry."
