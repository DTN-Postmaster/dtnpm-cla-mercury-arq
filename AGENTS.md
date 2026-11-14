# Agent Guide — `dtnpm-cla-mercury-arq`

## 🎯 Mission
Fournir l'adaptateur de couche de convergence (CLA) pour modems HF connectés Mercury ARQ dans l'écosystème DTN Postmaster, assurant l'interfaçage avec les sockets de contrôle et de données du modem, la résolution d'indicatifs radioamateurs en EID et l'ordonnancement des fenêtres de contact radio.

---

## 🛡️ Référentiels et Exigences de Sécurité

### 1. Cyber Resilience Act (CRA) & Sécurité HF
- **Validation d'Identité et Indicatifs Radioamateurs** :
  - Les indicatifs d'appel sont normalisés (`normalize_callsign`) et validés syntaxiquement (`is_valid_callsign` : 3 à 10 caractères, sans caractères de contrôle `\r`, `\n`, `\0`).
- **Opt-in Explicite pour les Tests Hors-Antenne** :
  - L'utilisation sans indicatif (ou avec `N0CALL`) émet un avertissement et n'est tolérée qu'en mode labo/dummy load via l'option `--allow-unidentified-rf` ou `require_callsign: false`.
  - Le drapeau `--require-callsign` permet d'interdire tout démarrage sans indicatif légitime.
- **Supply-Chain & VEX** :
  - Audit `cargo-audit` et `cargo-deny` ; VEX `not_affected` pour `RUSTSEC-2026-0459`.
  - Actions CI épinglées par SHA-1 à 40 caractères.
- **Politique de sécurité** : Consignée dans [SECURITY.md](SECURITY.md).

### 2. Robustesse Protocolaire & Prévention des Deadlocks
- **Résolution des Blocages d'État ARQ** :
  - Réinitialisation systématique de la session (`SessionState::Disconnected`) en cas de ligne de contrôle `"WRONG"` du modem, avec déblocage des promesses de connexion ou déconnexion en attente (prévention du deadlock permanent `Error::Busy`).
  - Détection et purge automatique des blocages dans les états transitoires `Connecting` (timeout par défaut de 30s) et `Disconnecting` (timeout de 5s).
  - Timeout dur de 5s enveloppant `MercuryCla::disconnect`.
- **Routage et Bornage des Ressources** :
  - Plafond maximal de trame `MAX_MERCURY_FRAME` fixé à 16 Mio avec vérification préalable avant mise en mémoire tampon.
  - Vérification obligatoire de correspondance entre l'adresse cible du bundle (`cla_addr`) et l'indicatif du pair actuellement connecté (`target == *peer_callsign`) avant toute émission, renvoyant `ForwardBundleResult::NoNeighbour` en cas d'incohérence.
  - Table des pairs bornée à 256 entrées maximum (`MAX_PEERS`).

---

## 🦀 Standards d'Ingénierie Rust

1. **Règle Zéro Panic / Zéro Unwrap** :
   - Tout le code sous `src/` doit être 100% exempt de `.unwrap()`, `.expect()` et de macro `panic!`.
   - Linter strict : `-D warnings -D clippy::unwrap_used -D clippy::panic`.
2. **ADRs Obligatoires** :
   - Toute décision architecturale Mercury ARQ doit être consignée sous `adr/` et indexée dans [adr/README.md](adr/README.md).

---

## ⚡ Commandes Critiques de Vérification

- `cargo fmt --all -- --check`
- `cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic`
- `cargo test --offline --all-features`
- `./scripts/check-security.sh --strict`
- `./scripts/check-coverage.sh --summary-only`
