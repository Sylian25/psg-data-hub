"""
PSG DATA HUB - Génération de la base (v2, mise à jour au 9 novembre 2026)
=========================================================================
Couvre l'ère QSI : saisons 2011-12 à 2026-27 (2026-27 en cours, arrêtée à la J7 de L1).
Stats joueurs = toutes compétitions confondues.

Chaque ligne de stats porte un indicateur de fiabilité (colonne data_quality) :
  - verifie : valeur relevée dans une source (Wikipedia, psg.fr)
  - estime  : valeur approximative (reconstituée pour illustrer, à ne pas citer)
  - partiel : saison en cours, valeur incomplète
Les champs inconnus sont laissés vides (NULL) plutôt qu'inventés.
"""
import json, os, sqlite3
import pandas as pd

# --- SAISONS (source : Wikipedia "List of PSG seasons"; goals = buts toutes compétitions) ---
# (id, label, rang L1, étape LDC, Coupe de France, Coupe de la Ligue, meilleur buteur, buts du club, statut)
seasons_data = [
    (1,  "2011-12", 2, "Non qualifié",  "QF",  "8es", "Nenê",         27, 99,  "verifie"),
    (2,  "2012-13", 1, "Quarts",        "QF",  "QF",  "Ibrahimović",  35, 101, "verifie"),
    (3,  "2013-14", 1, "Quarts",        "32es","V",   "Ibrahimović",  41, 126, "verifie"),
    (4,  "2014-15", 1, "Quarts",        "V",   "V",   "Cavani",       31, 122, "verifie"),
    (5,  "2015-16", 1, "Quarts",        "V",   "V",   "Ibrahimović",  50, 143, "verifie"),
    (6,  "2016-17", 2, "Huitièmes",     "V",   "V",   "Cavani",       49, 141, "verifie"),
    (7,  "2017-18", 1, "Huitièmes",     "V",   "V",   "Cavani",       40, 171, "verifie"),
    (8,  "2018-19", 1, "Huitièmes",     "F",   "QF",  "Mbappé",       39, 149, "verifie"),
    (9,  "2019-20", 1, "Finale",        "V",   "V",   "Mbappé",       30, 136, "verifie"),
    (10, "2020-21", 2, "Demi-finales",  "V",   None,  "Mbappé",       42, 126, "verifie"),
    (11, "2021-22", 1, "Huitièmes",     "8es", None,  "Mbappé",       39, 112, "verifie"),
    (12, "2022-23", 1, "Huitièmes",     "8es", None,  "Mbappé",       41, 120, "verifie"),
    (13, "2023-24", 1, "Demi-finales",  "V",   None,  "Mbappé",       44, 124, "verifie"),
    (14, "2024-25", 1, "Vainqueur",     "V",   None,  "Dembélé",      35, 168, "verifie"),
    (15, "2025-26", 1, "Vainqueur",     "32es",None,  "Dembélé",      20, 128, "verifie"),
    (16, "2026-27", 2, "En cours",      "En cours", None, "Ferran Torres", 4, None, "partiel"),
]
df_seasons = pd.DataFrame(seasons_data, columns=[
    "season_id","label","ligue1_rank","ucl_stage","coupe_de_france","coupe_de_la_ligue",
    "top_scorer","top_scorer_goals","goals_scored","data_quality"])

# --- JOUEURS ---
players_data = [
    (1,"Zlatan Ibrahimović","Suède","ATT",1981,"D"),(2,"Edinson Cavani","Uruguay","ATT",1987,"D"),
    (3,"Thiago Silva","Brésil","DEF",1984,"D"),(4,"David Beckham","Angleterre","MID",1975,"D"),
    (5,"Maxwell","Brésil","DEF",1981,"G"),(6,"Blaise Matuidi","France","MID",1987,"D"),
    (7,"Marco Verratti","Italie","MID",1992,"D"),(8,"Marquinhos","Brésil","DEF",1994,"D"),
    (9,"David Luiz","Brésil","DEF",1987,"D"),(10,"Serge Aurier","Côte d'Ivoire","DEF",1992,"D"),
    (11,"Neymar Jr","Brésil","ATT",1992,"D"),(12,"Kylian Mbappé","France","ATT",1998,"D"),
    (13,"Lionel Messi","Argentine","ATT",1987,"G"),(14,"Ángel Di María","Argentine","MID",1988,"G"),
    (15,"Mauro Icardi","Argentine","ATT",1993,"D"),(16,"Leandro Paredes","Argentine","MID",1994,"D"),
    (17,"Keylor Navas","Costa Rica","GK",1986,"D"),(18,"Achraf Hakimi","Maroc","DEF",1998,"D"),
    (19,"Presnel Kimpembe","France","DEF",1995,"D"),(20,"Idrissa Gueye","Sénégal","MID",1989,"D"),
    (21,"Ousmane Dembélé","France","ATT",1997,"D/G"),(22,"Bradley Barcola","France","ATT",2002,"G"),
    (23,"Vitinha","Portugal","MID",2000,"D"),(24,"Warren Zaïre-Emery","France","MID",2006,"D"),
    (25,"Gianluigi Donnarumma","Italie","GK",1999,"D"),(26,"Fabián Ruiz","Espagne","MID",1996,"G"),
    (27,"Nuno Mendes","Portugal","DEF",2002,"G"),(28,"Gonçalo Ramos","Portugal","ATT",2001,"D"),
    (29,"João Neves","Portugal","MID",2004,"D"),(30,"Willian Pacho","Équateur","DEF",2001,"D"),
    (31,"Désiré Doué","France","ATT",2005,"D"),(32,"Khvicha Kvaratskhelia","Géorgie","ATT",2001,"G"),
    (33,"Illia Zabarnyi","Ukraine","DEF",2002,"D"),(34,"Lucas Chevalier","France","GK",2001,"D"),
    (35,"Matvey Safonov","Russie","GK",1999,"D"),
    (36,"Maghnes Akliouche","France","MID",2002,None),(37,"Lucas Digne","France","DEF",1993,None),
    (38,"Ferran Torres","Espagne","ATT",2000,None),(39,"Mika Godts","Belgique","ATT",2005,None),
    (40,"Dro Fernández","Espagne","MID",2008,None),(41,"Lee Kang-in","Corée du Sud","MID",2001,None),
    (42,"Randal Kolo Muani","France","ATT",1998,None),(43,"Milan Škriniar","Slovaquie","DEF",1995,None),
    (44,"Marco Asensio","Espagne","ATT",1996,None),(45,"Nordi Mukiele","France","DEF",1997,None),
    (46,"Carlos Soler","Espagne","MID",1997,None),
]
df_players = pd.DataFrame(players_data, columns=["player_id","name","nationality","position","birth_year","foot"])

# --- TRANSFERTS ---  (id, joueur, saison, sens, de, vers, M€, type)
T = [
 (1,"IN",  "AC Milan","PSG",20.0,"Achat",2),(4,"IN","LA Galaxy","PSG",0.0,"Libre",2),
 (3,"IN",  "AC Milan","PSG",42.0,"Achat",2),(7,"IN","Pescara","PSG",12.0,"Achat",2),
 (2,"IN",  "Napoli","PSG",64.5,"Achat",3),(8,"IN","AS Roma","PSG",31.4,"Achat",3),
 (9,"IN",  "Chelsea","PSG",49.5,"Achat",4),(14,"IN","Real Madrid","PSG",63.0,"Achat",5),
 (11,"IN", "FC Barcelone","PSG",222.0,"Achat",7),(12,"IN","AS Monaco","PSG",0.0,"Prêt",7),
 (12,"IN", "AS Monaco","PSG",180.0,"Achat",8),(17,"IN","Real Madrid","PSG",15.0,"Achat",9),
 (13,"IN", "FC Barcelone","PSG",0.0,"Libre",11),(25,"IN","AC Milan","PSG",0.0,"Libre",11),
 (18,"IN", "Inter Milan","PSG",68.0,"Achat",11),(23,"IN","FC Porto","PSG",41.5,"Achat",12),
 (21,"IN", "FC Barcelone","PSG",50.0,"Achat",13),(22,"IN","Olympique Lyonnais","PSG",45.0,"Achat",13),
 (28,"IN", "Benfica","PSG",65.0,"Achat",13),(29,"IN","Benfica","PSG",59.8,"Achat",14),
 (30,"IN", "Eintracht Francfort","PSG",40.0,"Achat",14),(31,"IN","Stade Rennais","PSG",50.0,"Achat",14),
 (32,"IN", "SSC Napoli","PSG",70.0,"Achat",14),
 (34,"IN", "LOSC Lille","PSG",40.0,"Achat",15),(33,"IN","AFC Bournemouth","PSG",63.0,"Achat",15),
 (35,"IN", "FK Krasnodar","PSG",20.0,"Achat",15),(40,"IN","FC Barcelone","PSG",8.2,"Achat",15),
 (36,"IN", "AS Monaco","PSG",50.0,"Achat",16),(37,"IN","Aston Villa","PSG",7.0,"Achat",16),
 (38,"IN", "FC Barcelone","PSG",48.5,"Achat",16),(39,"IN","Ajax","PSG",45.0,"Achat",16),
 (1,"OUT","PSG","Manchester United",0.0,"Libre",6),(9,"OUT","PSG","Chelsea",34.0,"Achat",6),
 (4,"OUT","PSG","Retraite",0.0,"Libre",2),(2,"OUT","PSG","Manchester United",0.0,"Libre",10),
 (3,"OUT","PSG","Chelsea",0.0,"Libre",10),(14,"OUT","PSG","Juventus",0.0,"Libre",12),
 (11,"OUT","PSG","Al-Hilal",90.0,"Achat",13),(13,"OUT","PSG","Inter Miami",0.0,"Libre",13),
 (7,"OUT","PSG","Al-Arabi SC",45.0,"Achat",13),(12,"OUT","PSG","Real Madrid",0.0,"Libre",14),
 (25,"OUT","PSG","Manchester City",30.0,"Achat",15),(43,"OUT","PSG","Fenerbahçe",7.0,"Achat",15),
 (44,"OUT","PSG","Fenerbahçe",7.5,"Achat",15),(45,"OUT","PSG","Sunderland",11.0,"Achat",15),
 (46,"OUT","PSG","Real Sociedad",8.0,"Achat",15),(19,"OUT","PSG","Qatar SC",0.0,"Achat",15),
 (28,"OUT","PSG","AC Milan",74.0,"Achat",16),(41,"OUT","PSG","Atlético Madrid",35.0,"Achat",16),
 (42,"OUT","PSG","Juventus",38.0,"Achat",16),(22,"OUT","PSG","Liverpool",125.0,"Achat",16),
]
df_transfers = pd.DataFrame(
    [(i+1, p, s, d, f, t, fee, ty) for i,(p,d,f,t,fee,ty,s) in enumerate(T)],
    columns=["transfer_id","player_id","season_id","direction","from_club","to_club","fee_m_eur","transfer_type"])
df_transfers["data_quality"] = "verifie"
# Les montants des transferts 2011-2024 viennent de ma mémoire et de Wikipedia : à recouper avec Transfermarkt.
df_transfers.loc[df_transfers.season_id <= 14, "data_quality"] = "a_recouper"

# --- STATS JOUEURS (toutes compétitions) ---
rows = []
def add(pid, season, apps, goals, assists, q):
    rows.append((pid, season, apps, goals, assists, None, None, q))

# Ibrahimović : 156 buts (35+41+30+50) ; 2013-14 = 41 (Wikipedia). Ajusté : 35,41,30,50 = 156
for s,g in [(2,35),(3,41),(4,30),(5,50)]: add(1,s,None,g,None,"verifie")
# Cavani : 200 buts au total, 31 (2014-15), 49 (2016-17), 40 (2017-18) vérifiés ; autres saisons estimées pour atteindre 200
for s,g,q in [(3,25,"estime"),(4,31,"verifie"),(5,19,"estime"),(6,49,"verifie"),(7,40,"verifie"),
              (8,24,"estime"),(9,8,"estime"),(10,4,"estime")]: add(2,s,None,g,None,q)
# Mbappé : 256 buts (21 + 39 + 30 + 42 + 39 + 41 + 44)
for s,g in [(7,21),(8,39),(9,30),(10,42),(11,39),(12,41),(13,44)]: add(12,s,None,g,None,"verifie")
# Neymar : 118 buts (somme estimée)
for s,g,a in [(7,28,16),(8,23,13),(9,19,12),(10,19,10),(11,13,8),(12,9,7),(13,7,6)]: add(11,s,None,g,a,"estime")
# Dembélé / Barcola 2024-25 (psg.fr), 2025-26 (Wikipedia)
add(21,14,None,35,None,"verifie"); add(22,14,None,21,None,"verifie")
add(21,15,40,20,None,"verifie"); add(32,15,48,19,None,"verifie")
add(22,15,49,13,None,"verifie"); add(31,15,41,13,None,"verifie"); add(28,15,45,12,None,"verifie")
# 2026-27 (arrêté à la J7, 18 octobre 2026) : seuls les buteurs cités par la source
add(38,16,None,4,None,"partiel"); add(8,16,None,3,None,"partiel")

df_stats = pd.DataFrame(rows, columns=["player_id","season_id","appearances","goals","assists","xG","minutes","data_quality"])
df_stats.insert(0,"stat_id",range(1,len(df_stats)+1))

# --- PALMARÈS ---
won = {
 "Ligue 1": {x[0] for x in seasons_data if x[2]==1},
 "Coupe de France": {4,5,6,7,9,10,13,14},
 "Coupe de la Ligue": {3,4,5,6,7,9},
 "Ligue des champions": {14,15},
 "Trophée des Champions": {3,4,5,6,7,8,9,10,12,13,14,15},
 "Supercoupe de l'UEFA": {15,16},
 "Coupe intercontinentale": {15},
}
played = {"Coupe de la Ligue": range(1,10), "Trophée des Champions": range(3,17),
          "Supercoupe de l'UEFA": range(14,17), "Coupe intercontinentale": range(15,16)}
tr = []
for comp, wins in won.items():
    for s in played.get(comp, range(1,16)):
        tr.append((s, comp, int(s in wins)))
tr.append((16,"Trophée des Champions",0))   # perdu 0-1 le 16 août 2026
df_trophies = pd.DataFrame(tr, columns=["season_id","competition","won"]).drop_duplicates(["season_id","competition"])
df_trophies.insert(0,"trophy_id",range(1,len(df_trophies)+1))
df_trophies["data_quality"] = "verifie"

# --- SAUVEGARDE ---
os.makedirs("data/raw",exist_ok=True); os.makedirs("data/processed",exist_ok=True); os.makedirs("dashboard/assets",exist_ok=True)
tables = [(df_seasons,"seasons"),(df_players,"players"),(df_transfers,"transfers"),(df_stats,"player_stats"),(df_trophies,"trophies")]
for df,n in tables: df.to_csv(f"data/raw/{n}.csv",index=False)
if os.path.exists("data/psg_data.db"): os.remove("data/psg_data.db")
conn = sqlite3.connect("data/psg_data.db")
for df,n in tables: df.to_sql(n,conn,index=False)

def q(sql):
    df = pd.read_sql(sql,conn)
    return df.astype(object).where(df.notna(), None).to_dict(orient="records")  # NaN -> null (JSON valide)
mercato = []
for r in df_seasons.itertuples():
    t = df_transfers[df_transfers.season_id==r.season_id]
    d = round(float(t[t.direction=="IN"].fee_m_eur.sum()),1); c = round(float(t[t.direction=="OUT"].fee_m_eur.sum()),1)
    mercato.append({"season":r.label,"depenses":d,"recettes":c,"bilan":round(c-d,1)})
out = {
 "generatedAt":"2026-11-09",
 "mercato":mercato,
 "topScorers": q("""SELECT p.name, SUM(ps.goals) total_goals FROM player_stats ps JOIN players p USING(player_id)
                    GROUP BY p.name ORDER BY total_goals DESC LIMIT 10"""),
 "palmares": q("""SELECT competition, SUM(won) titres FROM trophies GROUP BY competition ORDER BY titres DESC"""),
 "seasons": q("SELECT * FROM seasons ORDER BY season_id"),
 "squad2526": q("""SELECT p.name,p.position,p.nationality,ps.appearances,ps.goals FROM player_stats ps
                   JOIN players p USING(player_id) WHERE ps.season_id=15 ORDER BY ps.goals DESC"""),
 "arrivals2026": q("""SELECT p.name,p.position,p.nationality,t.from_club,t.fee_m_eur FROM transfers t
                   JOIN players p USING(player_id) WHERE t.season_id=16 AND t.direction='IN' ORDER BY t.fee_m_eur DESC"""),
 "departures2026": q("""SELECT p.name,p.position,t.to_club,t.fee_m_eur FROM transfers t
                   JOIN players p USING(player_id) WHERE t.season_id=16 AND t.direction='OUT' ORDER BY t.fee_m_eur DESC"""),
 "nationalities": q("SELECT nationality, COUNT(*) count FROM players GROUP BY nationality ORDER BY count DESC LIMIT 8"),
 "kpi": q("""SELECT (SELECT COUNT(*) FROM players) players,(SELECT COUNT(*) FROM transfers) transfers,
             (SELECT COUNT(*) FROM seasons) seasons,
             (SELECT SUM(goals) FROM player_stats WHERE player_id=12) mbappe"""),
}
conn.close()
json.dump(out,open("dashboard/assets/psg_data.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)
for df,n in tables: print(n,len(df))
print(pd.DataFrame(out["topScorers"]).to_string(index=False)); print(out["palmares"]); print(out["kpi"])
