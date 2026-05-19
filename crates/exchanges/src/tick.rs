//! Format d'enregistrement partagé entre le `ws_recorder` et les outils d'analyse.
//!
//! Un fichier d'enregistrement est au format JSONL : une ligne = un [`Tick`]
//! sérialisé en JSON.

use serde::{Deserialize, Serialize};

/// Un niveau de carnet d'ordres : un prix et la quantité disponible à ce prix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    /// Prix du niveau.
    pub price: f64,
    /// Quantité (taille) disponible à ce prix.
    pub size: f64,
}

/// Un tick enregistré : un instantané du haut du carnet d'une venue à un instant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    /// Nanosecondes monotones depuis le démarrage du recorder.
    /// Sert à mesurer les intervalles entre ticks et le jitter.
    pub recv_ns: u64,
    /// Horodatage local de réception (epoch ms).
    pub recv_ms: u64,
    /// Horodatage serveur fourni par la venue (epoch ms ; 0 si absent).
    /// La différence `recv_ms - server_ms` donne la latence de transport.
    pub server_ms: u64,
    /// Nom de la venue : `"lighter"` ou `"hyperliquid"`.
    pub exchange: String,
    /// Symbole de l'actif : `"BTC"`, `"ETH"`, ...
    pub symbol: String,
    /// Numéro de séquence de la venue (`nonce` Lighter ; 0 si la venue n'en fournit pas).
    pub seq: u64,
    /// Séquence attendue du message précédent (`begin_nonce` Lighter ; 0 si absent).
    /// Permet de détecter les trous de séquence : `prev_seq` doit valoir le `seq`
    /// du tick précédent.
    pub prev_seq: u64,
    /// `true` si ce tick provient d'un snapshot complet, `false` si d'un delta.
    pub is_snapshot: bool,
    /// Meilleurs bids, triés par prix décroissant.
    pub bids: Vec<Level>,
    /// Meilleurs asks, triés par prix croissant.
    pub asks: Vec<Level>,
}
