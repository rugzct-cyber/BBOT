# RECON.md — Phase de reconnaissance BBOT

Bot d'arbitrage de spread entre **Lighter** et **Hyperliquid** (perp DEX).

Ce document liste les **questions ouvertes** auxquelles la phase de reconnaissance
doit répondre **avant** d'écrire le bot. Il est vivant : on remplit le champ
`Réponse:` au fur et à mesure.

### Légende des statuts

- `[CONNU]` — fait établi (code existant, expérience, API stable). Fiable.
- `[DOCUMENTÉ]` — annoncé par la doc officielle, **à confirmer par mesure réelle**.
- `[À MESURER]` — aucune donnée, c'est le travail de la recon.

Tant qu'une question n'a pas de réponse `[CONNU]` ou mesurée, elle reste une
hypothèse — pas une décision d'architecture.

---

## Outils de recon prévus

- `ws_recorder` — enregistre chaque tick WS des 2 venues (**L2 complet**) avec timestamp
  local haute précision **+ timestamp serveur + numéro de séquence**.
  Base : le `tick_recorder` de bot5 (reader Lighter à étendre, reader Hyperliquid à écrire).
- `feed_analyzer` — analyse les enregistrements : cadence, jitter, distribution des intervalles.
- `spread_replay` — rejoue les ticks, calcule le spread + VWAP, teste les filtres de fraîcheur.
- Appel one-shot — récupère les specs de contrat (tick size, lot size, taille mini).

---

## 1. Flux WebSocket (microstructure)

- [ ] Cadence réelle de chaque WS : ticks/s médian **et** pics ?
  - Réponse Lighter : `[DOCUMENTÉ]` canal `order_book/{market_id}` → maj toutes les ~50 ms (~20/s). À confirmer.
  - Réponse Hyperliquid : `[À MESURER]` — cadence du canal `l2Book` inconnue.
- [ ] Les feeds sont-ils réguliers ou par rafales ? Distribution des intervalles entre ticks ?
  - Réponse : `[À MESURER]` — Lighter annonce un pas fixe de 50 ms, à vérifier ; HL inconnu.
- [ ] Chaque venue fournit-elle un timestamp serveur dans ses messages ? Avec quelle précision ?
  - Réponse : `[CONNU]` Oui pour les deux. Lighter = champ `timestamp` (epoch ms). Hyperliquid = champ `time` (epoch ms).
- [ ] Latence de transport mesurée : timestamp serveur → réception locale ?
  - Réponse : `[À MESURER]` — possible sur les 2 venues grâce aux timestamps serveur ci-dessus.
- [ ] WS = snapshot initial + deltas, ou snapshots complets ? Y a-t-il des numéros de séquence ?
  - Réponse Lighter : `[CONNU]` snapshot complet au `subscribe`, puis deltas (~50 ms). Séquence via `nonce` / `begin_nonce` ; continuité = `begin_nonce` courant == `nonce` précédent. `offset` peut sauter en cas de reconnexion sur un autre serveur.
  - Réponse Hyperliquid : `[À MESURER]` — `l2Book` snapshot ou delta ? présence d'un numéro de séquence ?
- [ ] Combien de niveaux de carnet chaque WS expose-t-il ? Assez pour le VWAP à taille max ?
  - Réponse Lighter : `[À MESURER]` — profondeur de l'`order_book` à confirmer.
  - Réponse Hyperliquid : `[À MESURER]` — profondeur du `l2Book` à confirmer (le canal `bbo` ne donne qu'1 niveau).
- [ ] Quel canal met à jour le plus vite : L2, trades, BBO, mark price ?
  - Réponse : `[À MESURER]` — on enregistre le L2 ; comparer aux autres canaux si besoin.

## 2. Comparabilité des prix

- [ ] Quel prix comparer entre les 2 venues pour détecter un spread : BBO mid ? mark ? VWAP exécutable ?
  - Réponse : `[À DÉCIDER]` — hypothèse de travail : **VWAP exécutable** à taille cible, calculé sur le L2. À valider via `spread_replay`.
- [ ] Spread bid/ask natif typique par venue et par paire ? (il mange le profit)
  - Réponse : `[À MESURER]`

## 3. Staleness — le bug du décalage de feed

- [ ] À partir de quel âge un tick doit-il être considéré « périmé », par venue ?
  - Réponse : `[À MESURER]` — dépend de la cadence réelle de chaque feed (cf. §1).
- [ ] Avec un staleness gate, combien de faux signaux disparaissent (mesuré sur `spread_replay`) ?
  - Réponse : `[À MESURER]`
- [ ] Quelle fraction des spreads observés survit quand on exige les 2 jambes fraîches ?
  - Réponse : `[À MESURER]`

## 4. Exécution

- [ ] Types d'ordres acceptés par venue : IOC, post-only, reduce-only, market ?
  - Réponse Lighter : `[CONNU]` order_type : limit=0, market=1. TIF : IOC=0, GTT=1, POST_ONLY=2. `reduce_only` supporté. Ordre via WS `jsonapi/sendtx` (le plus rapide) ou REST `sendTx`.
  - Réponse Hyperliquid : `[À MESURER]` — types d'ordres et TIF à documenter.
- [ ] Rate limits API de placement/annulation ?
  - Réponse Lighter : `[À MESURER]`
  - Réponse Hyperliquid : `[DOCUMENTÉ]` ~1000 souscriptions, 2000 msg/min ; HL coupe la connexion en cas de rafale → délai ~50 ms entre messages. Limites de placement d'ordre à confirmer.
- [ ] RTT placement→ack et placement→fill mesurés ?
  - Réponse Lighter : `[PARTIEL]` `bench_lighter_ws` mesure sign + envoi WS (min/median/p95). Manque le temps jusqu'au fill ack.
  - Réponse Hyperliquid : `[À MESURER]`
- [ ] Comportement des ordres partiellement remplis ?
  - Réponse : `[À MESURER]`

## 5. Coûts / rentabilité

- [ ] Fréquence et calcul du funding sur chaque venue ?
  - Réponse Lighter : `[DOCUMENTÉ]` endpoint `/api/v1/funding-rates`. Fréquence/calcul exacts à confirmer.
  - Réponse Hyperliquid : `[À MESURER]`
- [ ] Frais maker/taker par venue ?
  - Réponse Lighter : `[CONNU]` tier standard = **0 / 0** (maker/taker) ; tiers premium = frais réduits étagés.
  - Réponse Hyperliquid : `[CONNU par l'utilisateur]` — à reporter dans VENUES.md.
- [ ] Sur un échantillon de spreads détectés, combien restent rentables après frais + slippage VWAP + funding ?
  - Réponse : `[À MESURER]`
- [ ] Durée de détention typique d'une position (décide si le funding compte ou est négligeable) ?
  - Réponse : `[À MESURER]`

## 6. Risque

- [ ] Calcul du prix de liquidation par venue ?
  - Réponse Lighter : `[DOCUMENTÉ]` champ `liquidation_price` dans les positions ; `initial_margin_fraction`, `maintenance_margin_fraction`, `closeout_margin_fraction` dans `orderBookDetails`.
  - Réponse Hyperliquid : `[À MESURER]`
- [ ] Marge/leverage : combien de collatéral pour une position delta-neutre de taille X ?
  - Réponse : `[À MESURER]`
- [ ] Clock skew entre les deux venues ?
  - Réponse : `[À MESURER]` — comparable une fois les timestamps serveur enregistrés (cf. §1).
- [ ] Legging : une jambe remplie, pas l'autre — pire scénario de perte chiffré ?
  - Réponse : `[À MESURER]`
- [ ] Comportement si coupure WS alors qu'une position est ouverte ?
  - Réponse : `[À MESURER]`

---

## Faits de référence (extraits du code/doc existants)

### Lighter
- WS mainnet : `wss://mainnet.zklighter.elliot.ai/stream` — testnet : `wss://testnet.zklighter.elliot.ai/stream`
- REST mainnet : `https://mainnet.zklighter.elliot.ai`
- Souscription carnet : `{"type":"subscribe","channel":"order_book/{market_id}"}`
- Prix/tailles = entiers scalés par `price_decimals` / `size_decimals` (cf. `orderBookDetails`)
- Signature = Schnorr/Poseidon2 sur corps Goldilocks — **Rust pur** (crates `lighter-crypto` de bot5).
  ⚠️ Le doc `lighter_api_reference.md` §8 (ECDSA secp256k1) est **obsolète/faux**.

### Hyperliquid
- Souscription BBO : `{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}`
- Souscription L2 : `{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}`
- Format niveau (bbo et l2Book) : `{"px":"...","sz":"...","n":<nb ordres>}`
- Message : `{"channel":"...","data":{"coin":"...","time":<ms>,...}}`
- Ping : `{"method":"ping"}` → canal `pong`. Message de bienvenue en texte clair.
- Signature/placement d'ordre HL : non encore étudiés (EIP-712/secp256k1 — phase bot).
