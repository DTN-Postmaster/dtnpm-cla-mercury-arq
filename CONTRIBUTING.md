# Guide de Contribution — `dtnpm-cla-mercury-arq`

Ce guide présente les règles de développement et les exigences de sécurité pour le CLA Mercury Connected ARQ de `dtnpm-cla-mercury-arq`.

---

## 1. Principes Fondamentaux

- **Zéro Panic / Zéro Unwrap** : Aucun appel `.unwrap()` ou `.expect()` en code de production.
- **Sécurité et Robustesse ARQ** :
  - Tout état transitoire de connexion ou déconnexion doit être protégé par un minuteur d'expiration.
  - L'émission d'un bundle doit obligatoirement vérifier la correspondance stricte avec le pair radio connecté.
- **Traçabilité Architecturale (ADR)** :
  - Tout ajout ou ajustement protocolaire doit faire l'objet d'un fichier ADR sous `adr/` et être indexé dans `adr/README.md`.

---

## 2. Vérification Locale

```bash
cargo fmt --all -- --check
cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic
cargo test --offline --all-features
./scripts/check-security.sh --strict
```
