#!/usr/bin/env bash

echo "=================================================================="
echo "   M.A.C.E. (Momentum Autonomous Cognitive Engine) Control System "
echo "           [ Systemd Autonomous Deployment Mode ]                 "
echo "=================================================================="

echo "[*] Syncing Systemd service configurations..."
systemctl daemon-reload

echo "[*] Launching Crypto Shield (15m interval)..."
systemctl start mace-crypto-shield.service

echo "[*] Launching TradFi Shield (1m interval)..."
systemctl start mace-tradfi-shield.service

echo "[*] Launching Crypto Swarm (4h interval)..."
systemctl start mace-crypto-orchestrator.service

echo "[*] Launching Equities Swarm (1h interval)..."
systemctl start mace-equities-orchestrator.service

echo "[*] Launching TradFi News Guard (4h interval)..."
systemctl start mace-tradfi-news-guard.service

echo "=================================================================="
echo "[+] M.A.C.E. Fleet Launch Sequence Complete."
echo "=================================================================="
echo ""
echo "To view live logs for a specific agent, use:"
echo "  journalctl -u mace-crypto-shield -f"
echo "  journalctl -u mace-tradfi-shield -f"
echo "  journalctl -u mace-crypto-orchestrator -f"
echo "  journalctl -u mace-equities-orchestrator -f"
echo "  journalctl -u mace-tradfi-news-guard -f"
echo ""
echo "To check the health/status of all agents:"
echo "  systemctl status 'mace-*'"
echo "=================================================================="
