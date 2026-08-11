#!/usr/bin/env bash

echo "[*] Issuing shutdown command to M.A.C.E. Fleet..."
systemctl stop mace-hwm-updater.service
systemctl stop mace-crypto-shield.service
systemctl stop mace-tradfi-shield.service
systemctl stop mace-crypto-orchestrator.service
systemctl stop mace-equities-orchestrator.service
systemctl stop mace-tradfi-news-guard.service
systemctl stop mace-whales-scout.timer
systemctl stop mace-whales-scout.service

echo "[+] All M.A.C.E. processes safely terminated."
