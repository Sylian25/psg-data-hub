# PSG Data Hub

Projet personnel d'analyse de données sur le Paris Saint-Germain (ère QSI, 2011 à aujourd'hui) : base SQL, scripts Python, tableau de bord JavaScript.

**Site :** https://psg-data-hub.vercel.app
**Mise à jour des données :** 9 novembre 2026 (saison 2026-27 en cours, arrêtée à la 7e journée de Ligue 1)

> Projet éducatif sans affiliation avec le Paris Saint-Germain Football Club. Aucun logo ni photo officiels. Réalisé avec l'aide d'une IA (Claude) dans le cadre de ma formation en data analyse.

## Ce que contient le projet

| Dossier | Rôle |
|---|---|
| `scripts/generate_data.py` | Construit les CSV, la base SQLite et le JSON du site |
| `sql/schema.sql`, `sql/queries/analyses.sql` | Schéma de la base et 10 requêtes (jointures, agrégations, filtres) |
| `notebooks/02_analysis.py` | Nettoyage pandas et graphiques matplotlib |
| `dashboard/` | Site statique (HTML, CSS, Chart.js) déployé sur Vercel |
| `data/` | CSV bruts, données nettoyées, base `psg_data.db` |

## Fiabilité des données

Chaque ligne porte une colonne `data_quality` :

- `verifie` : valeur relevée dans une source (Wikipedia, psg.fr)
- `estime` : approximation, à ne pas citer
- `partiel` : saison en cours
- `a_recouper` : montants de transferts avant 2025, à confirmer avec Transfermarkt

Les valeurs inconnues (passes décisives, xG, minutes) sont laissées vides au lieu d'être inventées. Les stats détaillées de 2026-27 se limitent aux buteurs cités par la source à la J7. Les résultats après le 18 octobre 2026 n'ont pas été recoupés.

## Lancer le projet

```bash
pip install pandas matplotlib numpy
python scripts/generate_data.py      # CSV + base SQLite + JSON
python notebooks/02_analysis.py      # graphiques
cd dashboard && python -m http.server # voir le site sur localhost:8000
```

## Auteur

AOUDIA Sylian · [LinkedIn](https://www.linkedin.com/in/sylian-aoudia-867411298/) · [Portfolio](https://sylian-aoudia-eportfolio.vercel.app/)
