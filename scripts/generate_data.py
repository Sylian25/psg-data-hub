"""
PSG Data Hub — Génération de données réalistes
Couvre l'ère QSI : 2011-2025
"""
import pandas as pd
import numpy as np
import json
import sqlite3
import os

np.random.seed(42)

# ─── SAISONS ────────────────────────────────────────────────────────────────
seasons_data = [
    (1,  "2011-12", 2, "Huitièmes",    "Ibrahimović", 68),
    (2,  "2012-13", 1, "Quarts",       "Ibrahimović", 69),
    (3,  "2013-14", 1, "Quarts",       "Ibrahimović", 84),
    (4,  "2014-15", 1, "Quarts",       "Ibrahimović", 72),
    (5,  "2015-16", 1, "Quarts",       "Ibrahimović", 75),
    (6,  "2016-17", 1, "Huitièmes",    "Cavani",      83),
    (7,  "2017-18", 1, "Huitièmes",    "Cavani",     108),
    (8,  "2018-19", 1, "Huitièmes",    "Mbappé",     105),
    (9,  "2019-20", 1, "Finale LDC",   "Neymar",      75),
    (10, "2020-21", 2, "Demi-finales", "Mbappé",      86),
    (11, "2021-22", 1, "Huitièmes",    "Mbappé",      90),
    (12, "2022-23", 1, "Huitièmes",    "Mbappé",      89),
    (13, "2023-24", 2, "Demi-finales", "Mbappé",      78),
    (14, "2024-25", 1, "Demi-finales", "Dembélé",     82),
]
df_seasons = pd.DataFrame(seasons_data, columns=[
    "season_id","label","ligue1_rank","ucl_stage","top_scorer","goals_scored"
])

# ─── JOUEURS ─────────────────────────────────────────────────────────────────
players_data = [
    (1,  "Zlatan Ibrahimović",   "Suède",       "ATT", 1981, "D"),
    (2,  "Edinson Cavani",       "Uruguay",     "ATT", 1987, "D"),
    (3,  "Thiago Silva",         "Brésil",      "DEF", 1984, "D"),
    (4,  "David Beckham",        "Angleterre",  "MID", 1975, "D"),
    (5,  "Maxwell",              "Brésil",      "DEF", 1981, "G"),
    (6,  "Blaise Matuidi",       "France",      "MID", 1987, "D"),
    (7,  "Marco Verratti",       "Italie",      "MID", 1992, "D"),
    (8,  "Marquinhos",           "Brésil",      "DEF", 1994, "D"),
    (9,  "David Luiz",           "Brésil",      "DEF", 1987, "D"),
    (10, "Serge Aurier",         "Côte d'Ivoire","DEF",1992, "D"),
    (11, "Neymar Jr",            "Brésil",      "ATT", 1992, "D"),
    (12, "Kylian Mbappé",        "France",      "ATT", 1998, "D"),
    (13, "Lionel Messi",         "Argentine",   "ATT", 1987, "D"),
    (14, "Ángel Di María",       "Argentine",   "MID", 1988, "D"),
    (15, "Mauro Icardi",         "Argentine",   "ATT", 1993, "D"),
    (16, "Leandro Paredes",      "Argentine",   "MID", 1994, "D"),
    (17, "Keylor Navas",         "Costa Rica",  "GK",  1986, "D"),
    (18, "Achraf Hakimi",        "Maroc",       "DEF", 1998, "D"),
    (19, "Presnel Kimpembe",     "France",      "DEF", 1995, "D"),
    (20, "Idrissa Gueye",        "Sénégal",     "MID", 1989, "D"),
    (21, "Ousmane Dembélé",      "France",      "ATT", 1997, "D"),
    (22, "Bradley Barcola",      "France",      "ATT", 2002, "G"),
    (23, "Vitinha",              "Portugal",    "MID", 2000, "D"),
    (24, "Warren Zaïre-Emery",   "France",      "MID", 2006, "D"),
    (25, "Gianluigi Donnarumma", "Italie",      "GK",  1999, "D"),
    (26, "Fabián Ruiz",          "Espagne",     "MID", 1996, "D"),
    (27, "Lucas Hernandez",      "France",      "DEF", 1996, "G"),
    (28, "Gonçalo Ramos",        "Portugal",    "ATT", 2001, "D"),
    (29, "João Neves",           "Portugal",    "MID", 2004, "D"),
    (30, "Willian Pacho",        "Équateur",    "DEF", 2001, "D"),
]
df_players = pd.DataFrame(players_data, columns=[
    "player_id","name","nationality","position","birth_year","foot"
])

# ─── TRANSFERTS ──────────────────────────────────────────────────────────────
transfers_data = [
    (1,  1,  1,  "IN",  "AC Milan",            "PSG",               12.0,  "Achat"),
    (2,  4,  1,  "IN",  "LA Galaxy",            "PSG",                0.0,  "Libre"),
    (3,  3,  2,  "IN",  "Chelsea",              "PSG",               42.0,  "Achat"),
    (4,  9,  2,  "IN",  "Chelsea",              "PSG",               49.5,  "Achat"),
    (5,  2,  3,  "IN",  "Napoli",               "PSG",               64.5,  "Achat"),
    (6,  7,  3,  "IN",  "Pescara",              "PSG",                9.0,  "Achat"),
    (7,  8,  4,  "IN",  "Roma",                 "PSG",               31.4,  "Achat"),
    (8,  6,  5,  "IN",  "PSG Academy",          "PSG",                0.0,  "Formation"),
    (9,  14, 6,  "IN",  "Juventus",             "PSG",               63.0,  "Achat"),
    (10, 11, 7,  "IN",  "FC Barcelone",         "PSG",              222.0,  "Achat"),
    (11, 12, 7,  "IN",  "AS Monaco",            "PSG",              145.0,  "Prêt"),
    (12, 17, 9,  "IN",  "Real Madrid",          "PSG",               15.0,  "Achat"),
    (13, 18, 10, "IN",  "Inter Milan",          "PSG",               60.0,  "Achat"),
    (14, 13, 11, "IN",  "FC Barcelone",         "PSG",                0.0,  "Libre"),
    (15, 12, 11, "IN",  "AS Monaco",            "PSG",              180.0,  "Achat"),
    (16, 25, 11, "IN",  "AC Milan",             "PSG",                0.0,  "Libre"),
    (17, 21, 13, "IN",  "FC Barcelone",         "PSG",               50.0,  "Achat"),
    (18, 22, 13, "IN",  "Olympique Lyonnais",   "PSG",               45.0,  "Achat"),
    (19, 23, 13, "IN",  "FC Porto",             "PSG",               41.5,  "Achat"),
    (20, 28, 14, "IN",  "Benfica",              "PSG",               65.0,  "Achat"),
    (21, 29, 14, "IN",  "Benfica",              "PSG",               70.0,  "Achat"),
    (22, 30, 14, "IN",  "Eintracht Frankfurt",  "PSG",               40.0,  "Achat"),
    # Départs
    (23, 1,  6,  "OUT", "PSG", "Manchester United",    0.0,  "Libre"),
    (24, 9,  5,  "OUT", "PSG", "FC Barcelone",        32.0,  "Achat"),
    (25, 4,  2,  "OUT", "PSG", "Retraite",             0.0,  "Libre"),
    (26, 14, 12, "OUT", "PSG", "Juventus",             0.0,  "Libre"),
    (27, 2,  12, "OUT", "PSG", "Atlético Madrid",      0.0,  "Libre"),
    (28, 3,  11, "OUT", "PSG", "Chelsea",               0.0,  "Libre"),
    (29, 11, 13, "OUT", "PSG", "Al-Hilal",             90.0,  "Achat"),
    (30, 12, 13, "OUT", "PSG", "Real Madrid",           0.0,  "Libre"),
    (31, 13, 13, "OUT", "PSG", "Inter Miami",           0.0,  "Libre"),
    (32, 7,  13, "OUT", "PSG", "Al-Arabi SC",           0.0,  "Libre"),
]
df_transfers = pd.DataFrame(transfers_data, columns=[
    "transfer_id","player_id","season_id","direction",
    "from_club","to_club","fee_m_eur","transfer_type"
])

# ─── STATS JOUEURS ───────────────────────────────────────────────────────────
stats_rows = []
sid = 1

def add_stats(player_id, season_id, apps, goals, assists, xg, minutes):
    global sid
    stats_rows.append((sid, player_id, season_id, apps, goals, assists, xg, minutes))
    sid += 1

# Ibrahimovic
for i, (s, g, a) in enumerate([(1,22,8),(2,30,9),(3,26,7),(4,19,8),(5,19,6)]):
    add_stats(1, s, 31+i, g, a, round(g*0.95,1), (31+i)*72)

# Cavani
for s, g, a, app in [(3,21,5,34),(4,22,6,35),(5,19,5,32),(6,26,6,35),(7,35,6,36),(8,20,5,28),(9,13,5,22),(10,9,4,16)]:
    add_stats(2, s, app, g, a, round(g*0.97,1), app*72)

# Verratti
for s in range(3, 14):
    add_stats(7, s, 30, 1, 6, 1.2, 30*75)

# Marquinhos
for s in range(4, 15):
    add_stats(8, s, 34, 3, 1, 2.8, 34*80)

# Neymar
for s, g, a, app in [(7,19,13,28),(8,13,8,17),(9,15,8,23),(10,13,6,18),(11,11,9,22),(12,6,5,14),(13,2,2,8)]:
    add_stats(11, s, app, g, a, round(g*1.05,1), app*74)

# Mbappé
for s, g, a, app in [(8,7,4,27),(9,18,7,26),(10,15,10,28),(11,27,17,34),(12,28,17,32),(13,29,7,29)]:
    add_stats(12, s, app, g, a, round(g*0.97,1), app*76)

# Messi
for s, g, a, app in [(11,11,15,26),(12,21,20,29)]:
    add_stats(13, s, app, g, a, round(g*1.02,1), app*61)

# Di María
for s, g, a, app in [(6,10,19,34),(7,9,17,31),(8,11,14,28),(9,8,17,24),(10,6,9,21),(11,6,7,19),(12,2,3,12)]:
    add_stats(14, s, app, g, a, round(g*0.92,1), app*70)

# Hakimi
for s, g, a, app in [(10,4,9,33),(11,5,8,35),(12,7,9,36),(13,4,5,34),(14,5,8,33)]:
    add_stats(18, s, app, g, a, round(g*0.95,1), app*82)

# Dembélé
for s, g, a, app in [(13,10,12,32),(14,16,20,35)]:
    add_stats(21, s, app, g, a, round(g*0.95,1), app*79)

# Barcola
for s, g, a, app in [(13,8,5,27),(14,17,12,33)]:
    add_stats(22, s, app, g, a, round(g*0.94,1), app*77)

# Vitinha
for s, g, a, app in [(13,3,8,36),(14,5,9,37)]:
    add_stats(23, s, app, g, a, 2.3, app*83)

# Zaïre-Emery
for s, g, a, app in [(13,4,5,28),(14,6,8,32)]:
    add_stats(24, s, app, g, a, round(g*0.9,1), app*75)

# Donnarumma
for s, app in [(11,37),(12,36),(13,35),(14,36)]:
    add_stats(25, s, app, 0, 0, 0.0, app*90)

# Gonçalo Ramos
add_stats(28, 14, 28, 13, 7, 14.2, 28*68)

# João Neves
add_stats(29, 14, 30, 3, 6, 2.8, 30*80)

df_stats = pd.DataFrame(stats_rows, columns=[
    "stat_id","player_id","season_id","appearances","goals","assists","xG","minutes"
])

# ─── PALMARÈS ─────────────────────────────────────────────────────────────────
trophies_rows = []
tid = 1
# Ligue 1
for s in df_seasons.itertuples():
    trophies_rows.append((tid, s.season_id, "Ligue 1", int(s.ligue1_rank == 1))); tid+=1
# Coupe de France
cf_wins = {1,2,3,4,5,6,7,8,9,11,12,13,14}
for s in df_seasons.itertuples():
    trophies_rows.append((tid, s.season_id, "Coupe de France", int(s.season_id in cf_wins))); tid+=1
# Coupe de la Ligue (supprimée 2020)
cl_wins = {1,2,3,4,5,6,7,8,9}
for s in df_seasons.itertuples():
    if s.season_id <= 9:
        trophies_rows.append((tid, s.season_id, "Coupe de la Ligue", int(s.season_id in cl_wins))); tid+=1
# Champions League
for s in df_seasons.itertuples():
    trophies_rows.append((tid, s.season_id, "Champions League", int(s.season_id == 9))); tid+=1

df_trophies = pd.DataFrame(trophies_rows, columns=["trophy_id","season_id","competition","won"])

# ─── SAUVEGARDE ──────────────────────────────────────────────────────────────
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("dashboard/assets", exist_ok=True)

df_seasons.to_csv("data/raw/seasons.csv",       index=False)
df_players.to_csv("data/raw/players.csv",       index=False)
df_transfers.to_csv("data/raw/transfers.csv",   index=False)
df_stats.to_csv("data/raw/player_stats.csv",    index=False)
df_trophies.to_csv("data/raw/trophies.csv",     index=False)
print("✅ CSV sauvegardés dans data/raw/")

# ─── BASE SQLITE ──────────────────────────────────────────────────────────────
conn = sqlite3.connect("data/psg_data.db")
for df, name in [(df_seasons,"seasons"),(df_players,"players"),
                  (df_transfers,"transfers"),(df_stats,"player_stats"),(df_trophies,"trophies")]:
    df.to_sql(name, conn, if_exists="replace", index=False)
print("✅ Base SQLite créée : data/psg_data.db")

# ─── EXPORT JSON ──────────────────────────────────────────────────────────────
mercato_json = []
for _, row in df_seasons.iterrows():
    t = df_transfers[df_transfers.season_id == row.season_id]
    dep = float(t[t.direction=="IN"]["fee_m_eur"].sum())
    rec = float(t[t.direction=="OUT"]["fee_m_eur"].sum())
    mercato_json.append({"season": row.label, "depenses": round(dep,1),
                          "recettes": round(rec,1), "bilan": round(rec-dep,1)})

top_scorers_df = pd.read_sql("""
    SELECT p.name, SUM(ps.goals) as total_goals, SUM(ps.assists) as total_assists,
           ROUND(SUM(ps.xG),1) as total_xG
    FROM player_stats ps JOIN players p ON ps.player_id=p.player_id
    GROUP BY p.name ORDER BY total_goals DESC LIMIT 10
""", conn)

nat_df = pd.read_sql("""
    SELECT nationality, COUNT(*) as count FROM players
    GROUP BY nationality ORDER BY count DESC LIMIT 12
""", conn)

current_squad_df = pd.read_sql("""
    SELECT p.name, p.position, p.nationality, p.birth_year,
           ps.goals, ps.assists, ps.appearances, ps.xG, ps.minutes
    FROM player_stats ps JOIN players p ON ps.player_id=p.player_id
    WHERE ps.season_id=14 ORDER BY ps.goals DESC
""", conn)

palmares_df = pd.read_sql("""
    SELECT competition, SUM(won) as titres FROM trophies
    GROUP BY competition ORDER BY titres DESC
""", conn)

seasons_goals_df = pd.read_sql("""
    SELECT label, goals_scored, ligue1_rank, ucl_stage FROM seasons ORDER BY season_id
""", conn)

conn.close()

data_export = {
    "mercato":       mercato_json,
    "topScorers":    top_scorers_df.to_dict(orient="records"),
    "nationalities": nat_df.to_dict(orient="records"),
    "currentSquad":  current_squad_df.to_dict(orient="records"),
    "palmares":      palmares_df.to_dict(orient="records"),
    "seasonsGoals":  seasons_goals_df.to_dict(orient="records"),
}

with open("dashboard/assets/psg_data.json", "w", encoding="utf-8") as f:
    json.dump(data_export, f, ensure_ascii=False, indent=2)
print("✅ JSON exporté : dashboard/assets/psg_data.json")

print("\n📊 Aperçu des données :")
for df, name in [(df_seasons,"seasons"),(df_players,"players"),
                  (df_transfers,"transfers"),(df_stats,"player_stats"),(df_trophies,"trophies")]:
    print(f"  {name}: {len(df)} lignes")
