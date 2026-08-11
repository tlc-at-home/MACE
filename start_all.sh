#!/usr/bin/env bash

echo "=================================================================="
echo "   M.A.C.E. (Momentum Autonomous Cognitive Engine) Control System "
echo "           [ Systemd Autonomous Deployment Mode ]                 "
echo "=================================================================="

echo "[*] Syncing Systemd service configurations..."
systemctl daemon-reload

echo "[*] Launching HWM & Volatility Stop Updater (1m interval)..."
systemctl enable --now mace-hwm-updater.service

echo "[*] Launching Crypto Shield (15m interval)..."
systemctl enable --now mace-crypto-shield.service

echo "[*] Launching TradFi Shield (1m interval)..."
systemctl enable --now mace-tradfi-shield.service

echo "[*] Launching Crypto Swarm (4h interval)..."
systemctl enable --now mace-crypto-orchestrator.service

echo "[*] Launching Equities Swarm (1h interval)..."
systemctl enable --now mace-equities-orchestrator.service

echo "[*] Launching TradFi News Guard (4h interval)..."
systemctl enable --now mace-tradfi-news-guard.service

echo "[*] Launching Whales & Political Disclosure Scout (Twice daily timer)..."
systemctl enable --now mace-whales-scout.timer

echo "=================================================================="
echo "[+] M.A.C.E. Fleet Launch Sequence Complete."
echo "=================================================================="
echo ""
echo "To view live logs for a specific agent, use:"
echo "  journalctl -u mace-hwm-updater -f"
echo "  journalctl -u mace-crypto-shield -f"
echo "  journalctl -u mace-tradfi-shield -f"
echo "  journalctl -u mace-crypto-orchestrator -f"
echo "  journalctl -u mace-equities-orchestrator -f"
echo "  journalctl -u mace-tradfi-news-guard -f"
echo "  journalctl -u mace-whales-scout -f"
echo ""
echo "To check the health/status of all agents:"
echo "  systemctl status 'mace-*'"
echo "=================================================================="
