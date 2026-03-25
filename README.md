# ⚽ PSG Data Hub

> Analyse de données du Paris Saint-Germain (ère QSI 2011–2025)  
> **Stack : Python · Pandas · NumPy · SQLite · Chart.js · HTML/CSS**

---

## 🎯 Présentation du projet

Ce projet est un portfolio data analytics centré sur le Paris Saint-Germain.  
Il couvre **14 saisons** (2011-2025), **30 joueurs**, **32 transferts** et **76 fiches statistiques** individuelles.

**Objectif :** Mettre en pratique Python (Pandas, NumPy), SQL (modélisation, requêtes analytiques) et la data visualisation, en appliquant une approche "Data Analyst RH" à la gestion d'un effectif de football.

🌐 **[→ Voir le dashboard en ligne](https://psg-data-hub.vercel.app/)*

---

## 📁 Structure du projet

```
psg-data-hub/
│
├── data/
│   ├── raw/                  ← CSV sources (seasons, players, transfers, stats, trophies)
│   ├── processed/            ← Données nettoyées (player_stats_clean.csv)
│   └── psg_data.db           ← Base de données SQLite
│
├── sql/
│   ├── schema.sql            ← DDL : définition des 5 tables
│   └── queries/
│       └── analyses.sql      ← 10 requêtes analytiques commentées
│
├── notebooks/
│   ├── 02_analysis.py        ← Nettoyage, exploration, visualisations Matplotlib
│   └── (Jupyter notebooks à venir)
│
├── scripts/
│   └── generate_data.py      ← Génération des données + export JSON
│
├── dashboard/
│   ├── index.html            ← Site web interactif (Chart.js)
│   └── assets/
│       ├── psg_data.json     ← Données exportées pour le frontend
│       ├── top_scorers.png
│       ├── mercato.png
│       ├── mbappe_evolution.png
│       └── squad_bubble.png
│
└── README.md
```

---

## 🗄️ Modèle de données (SQL)

5 tables relationnelles :

| Table | Description | Lignes |
|-------|-------------|--------|
| `seasons` | 14 saisons ère QSI, classements, top scoreur | 14 |
| `players` | Joueurs ayant porté le maillot, poste, nationalité | 30 |
| `transfers` | Tous les transferts IN/OUT avec montants | 32 |
| `player_stats` | Stats individuelles par saison (buts, passes, xG...) | 76 |
| `trophies` | Palmarès complet (Ligue 1, CDL, CdF, LDC) | 51 |

---

## 🐍 Pipeline Python

```
generate_data.py  →  data/raw/*.csv
                  →  data/psg_data.db  (SQLite)
                  →  dashboard/assets/psg_data.json

02_analysis.py    →  Nettoyage & audit qualité
                  →  4 visualisations Matplotlib exportées
```

---

## 🔍 Requêtes SQL clés

```sql
-- Top 10 buteurs all-time avec ratio buts/xG
SELECT p.name, SUM(ps.goals) AS total_buts,
       ROUND(SUM(ps.goals) * 1.0 / NULLIF(SUM(ps.xG), 0), 2) AS ratio_buts_xG
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
GROUP BY p.name ORDER BY total_buts DESC LIMIT 10;

-- Bilan mercato net par saison
SELECT s.label,
  ROUND(SUM(CASE WHEN t.direction='IN' THEN t.fee_m_eur ELSE 0 END),1) AS depenses,
  ROUND(SUM(CASE WHEN t.direction='OUT' THEN t.fee_m_eur ELSE 0 END),1) AS recettes
FROM transfers t JOIN seasons s ON t.season_id = s.season_id
GROUP BY s.label ORDER BY s.season_id;
```

→ [Voir les 10 requêtes complètes](sql/queries/analyses.sql)

---

## 🚀 Lancer le projet

```bash
# 1. Cloner
git clone https://github.com/Sylian25/psg-data-hub.git
cd psg-data-hub

# 2. Installer les dépendances
pip install pandas numpy matplotlib

# 3. Générer les données
python scripts/generate_data.py

# 4. Lancer l'analyse Python
python notebooks/02_analysis.py

# 5. Ouvrir le dashboard
cd dashboard && open index.html
# ou : python -m http.server 3000 → http://localhost:3000
```

---

## 📊 Analyses incluses

- 🏆 **Palmarès** : 14 saisons de titres (Ligue 1, CdF, CdL, LDC)
- ⚽ **Top buteurs all-time** : Cavani, Mbappé, Ibrahimović, Neymar...
- 📈 **xG vs buts réels** : efficacité des attaquants
- 💸 **Mercato** : dépenses, recettes, bilan net par saison
- 📅 **Timeline historique** : chaque saison résumée
- 🌍 **Nationalités recrutées** : angle Data RH
- 👥 **Effectif 2024-25** : stats actuelles filtrables par poste

---

## 🛠️ Stack technique

| Outil | Usage |
|-------|-------|
| Python 3 | Génération et nettoyage des données |
| Pandas | Manipulation des DataFrames |
| NumPy | Calculs statistiques |
| SQLite3 | Base de données relationnelle |
| Matplotlib | Visualisations statiques |
| Chart.js | Dashboard interactif |
| HTML/CSS/JS | Frontend du site |
| Vercel | Déploiement |

---

## 👤 Auteur

**Sylian AOUDIA** — Data Analyst RH en alternance @ BNP Paribas IT Group  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-blue?style=flat&logo=linkedin)](https://www.linkedin.com/in/sylian-aoudia-867411298/)
[![GitHub](https://img.shields.io/badge/GitHub-black?style=flat&logo=github)](https://github.com/Sylian25)
[![Portfolio](https://img.shields.io/badge/Portfolio-white?style=flat)](https://sylian-aoudia-eportfolio.vercel.app/)

---

> *"Without data, you're just another person with an opinion."* — W. Edwards Deming
