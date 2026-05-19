# BBOT

Bot d'arbitrage de spread entre **Lighter** et **Hyperliquid** (perp DEX).

## État : phase de reconnaissance

Avant tout développement du bot, une phase de mesure caractérise les deux
venues (cadence des flux WebSocket, latence, profondeur, rentabilité réelle).
Les questions ouvertes sont suivies dans `RECON.md`.

## Structure

- `crates/` — bibliothèques partagées (connexion WebSocket, types).
- `tools/` — programmes exécutables (outils de recon, puis le bot).
- `docs/` — documentation de référence des API.

## Build

    cargo build
