"""
PSG DATA HUB — Scraping FBref
==============================
Ce script récupère les vraies données du PSG depuis FBref.com
Données ciblées :
  - Stats d'équipe par saison (Ligue 1)
  - Stats joueurs saison en cours

IMPORTANT : on attend 4 secondes entre chaque requête
pour ne pas se faire bloquer par FBref.
"""

import pandas as pd
import requests
import time
import os
import sqlite3
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────
# On se fait passer pour un navigateur normal
# Sans ça, FBref peut refuser notre requête
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

PAUSE = 4  # secondes entre chaque requête (obligatoire)

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)


# ════════════════════════════════════════════════════════════════
# FONCTION UTILITAIRE : récupérer une page FBref
# ════════════════════════════════════════════════════════════════
def fetch_fbref(url: str, description: str) -> list[pd.DataFrame] | None:
    """
    Envoie une requête GET à FBref et retourne
    la liste des tableaux trouvés sur la page.

    Paramètres :
        url         : l'URL de la page FBref
        description : texte affiché dans le terminal

    Retourne :
        Liste de DataFrames (un par tableau sur la page)
        ou None si erreur
    """
    print(f"\n🔄 Scraping : {description}")
    print(f"   URL : {url}")

    try:
        # On attend avant d'envoyer la requête
        # (sauf pour la toute première)
        time.sleep(PAUSE)

        # Envoi de la requête HTTP GET
        response = requests.get(url, headers=HEADERS, timeout=15)

        # Vérification : est-ce que la requête a réussi ?
        # 200 = OK, 403 = bloqué, 404 = page inexistante
        print(f"   Status HTTP : {response.status_code}")

        if response.status_code != 200:
            print(f"   ❌ Erreur HTTP {response.status_code}")
            return None

        # pandas.read_html() lit TOUS les tableaux <table> de la page
        # Il retourne une liste de DataFrames
        tables = pd.read_html(response.text, encoding="utf-8")
        print(f"   ✅ {len(tables)} tableau(x) trouvé(s)")
        return tables

    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return None


# ════════════════════════════════════════════════════════════════
# 1. STATS JOUEURS PSG — SAISON 2025-26
# ════════════════════════════════════════════════════════════════
def scrape_psg_squad_stats():
    """
    Récupère les stats de l'effectif PSG pour la saison actuelle.
    URL FBref : page de l'équipe PSG en Ligue 1
    """
    # URL de la page PSG sur FBref (saison 2025-26)
    url = "https://fbref.com/en/squads/e2d8892c/Paris-Saint-Germain-Stats"

    tables = fetch_fbref(url, "Stats effectif PSG 2025-26")

    if tables is None:
        print("   ⚠️  Scraping échoué — utilisation des données de secours")
        return None

    # Le premier tableau est généralement les stats standard
    # On cherche celui qui contient "Goals" dans les colonnes
    for i, df in enumerate(tables):
        # Aplatir les colonnes multi-niveaux (FBref utilise des headers doubles)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join(col).strip() for col in df.columns]

        cols_str = ' '.join(df.columns.astype(str)).lower()

        if 'gls' in cols_str or 'goals' in cols_str or 'g+a' in cols_str:
            print(f"   📊 Tableau de stats trouvé (index {i})")
            print(f"   Colonnes : {list(df.columns[:10])}...")

            # Nettoyage de base
            df = df.copy()

            # Supprimer les lignes vides ou de total
            if 'Player' in df.columns:
                df = df[df['Player'].notna()]
                df = df[df['Player'] != 'Player']  # ligne d'en-tête dupliquée
                df = df[~df['Player'].str.contains('Squad Total|vs. Squad', na=False)]

            # Sauvegarde brute
            df.to_csv("data/raw/fbref_squad_2025_26_raw.csv", index=False, encoding="utf-8-sig")
            print(f"   ✅ Sauvegardé : data/raw/fbref_squad_2025_26_raw.csv ({len(df)} joueurs)")
            return df

    print("   ⚠️  Aucun tableau de stats reconnu")
    return None


# ════════════════════════════════════════════════════════════════
# 2. HISTORIQUE RÉSULTATS PSG EN LIGUE 1
# ════════════════════════════════════════════════════════════════
def scrape_psg_season_history():
    """
    Récupère l'historique des saisons PSG en Ligue 1.
    Page des saisons de l'équipe PSG.
    """
    url = "https://fbref.com/en/squads/e2d8892c/history/Paris-Saint-Germain-Season-History"

    tables = fetch_fbref(url, "Historique saisons PSG")

    if tables is None:
        return None

    for i, df in enumerate(tables):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [' '.join(col).strip() for col in df.columns]

        cols_str = ' '.join(df.columns.astype(str)).lower()

        if 'season' in cols_str or 'lg' in cols_str or 'ligue' in cols_str.lower():
            print(f"   📊 Historique trouvé (index {i})")

            df = df.copy()
            df = df[df.iloc[:, 0].notna()]

            df.to_csv("data/raw/fbref_season_history_raw.csv", index=False, encoding="utf-8-sig")
            print(f"   ✅ Sauvegardé : data/raw/fbref_season_history_raw.csv ({len(df)} saisons)")
            return df

    return None


# ════════════════════════════════════════════════════════════════
# 3. NETTOYAGE DES DONNÉES SCRAPÉES
# ════════════════════════════════════════════════════════════════
def clean_squad_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie le DataFrame brut de FBref pour le rendre exploitable.

    Opérations :
    - Renommage des colonnes
    - Conversion des types
    - Suppression des doublons
    - Remplacement des NaN
    """
    print("\n🧹 Nettoyage des données...")

    df = df.copy()

    # Dictionnaire de renommage : colonnes FBref → nos noms
    rename_map = {}
    for col in df.columns:
        col_lower = col.lower().replace(' ', '_')
        if 'player' in col_lower:        rename_map[col] = 'name'
        elif col_lower in ('gls', 'goals performance_gls', 'gls_standard'):
            rename_map[col] = 'goals'
        elif col_lower in ('ast', 'ast_standard', 'performance_ast'):
            rename_map[col] = 'assists'
        elif col_lower in ('mp', 'playing_time_mp'):
            rename_map[col] = 'appearances'
        elif col_lower in ('min', 'playing_time_min'):
            rename_map[col] = 'minutes'
        elif 'xg' in col_lower and 'xa' not in col_lower and 'npxg' not in col_lower:
            rename_map[col] = 'xG'
        elif 'pos' in col_lower and len(col) <= 10:
            rename_map[col] = 'position_raw'
        elif 'nation' in col_lower:
            rename_map[col] = 'nationality_raw'
        elif col_lower in ('age',):
            rename_map[col] = 'age'

    df = df.rename(columns=rename_map)

    # Colonnes essentielles avec valeurs par défaut
    for col, default in [('goals', 0), ('assists', 0), ('appearances', 0),
                          ('minutes', 0), ('xG', 0.0)]:
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Nettoyage nationalité (FBref met "fr FRA" → on garde "FRA")
    if 'nationality_raw' in df.columns:
        df['nationality'] = df['nationality_raw'].str.extract(r'([A-Z]{2,3})$')
    elif 'nationality' not in df.columns:
        df['nationality'] = 'N/A'

    # Normalisation position
    pos_map = {'FW': 'ATT', 'MF': 'MID', 'DF': 'DEF', 'GK': 'GK',
               'FW,MF': 'ATT', 'MF,FW': 'MID', 'DF,MF': 'DEF'}
    if 'position_raw' in df.columns:
        df['position'] = df['position_raw'].map(pos_map).fillna('MID')
    elif 'position' not in df.columns:
        df['position'] = 'MID'

    # Suppression doublons sur le nom
    if 'name' in df.columns:
        df = df.drop_duplicates(subset='name', keep='first')
        df = df[df['name'].notna() & (df['name'] != '')]

    # Sélection colonnes finales
    keep_cols = [c for c in ['name', 'position', 'nationality', 'age',
                               'appearances', 'goals', 'assists', 'xG', 'minutes']
                 if c in df.columns]
    df = df[keep_cols]

    # Tri par buts décroissants
    df = df.sort_values('goals', ascending=False).reset_index(drop=True)

    print(f"   ✅ Données nettoyées : {len(df)} joueurs")
    print(df[['name', 'goals', 'assists', 'appearances']].head(8).to_string())

    df.to_csv("data/processed/squad_stats_2025_26_clean.csv", index=False, encoding="utf-8-sig")
    print("   ✅ Sauvegardé : data/processed/squad_stats_2025_26_clean.csv")

    return df


# ════════════════════════════════════════════════════════════════
# 4. MISE À JOUR DE LA BASE SQLITE
# ════════════════════════════════════════════════════════════════
def update_sqlite(df_clean: pd.DataFrame):
    """
    Met à jour la base SQLite avec les données scrapées.
    Ajoute la saison 2025-26 si elle n'existe pas.
    """
    print("\n💾 Mise à jour SQLite...")

    conn = sqlite3.connect("data/psg_data.db")
    cursor = conn.cursor()

    # Vérifier si la saison 2025-26 existe déjà
    cursor.execute("SELECT season_id FROM seasons WHERE label = '2025-26'")
    existing = cursor.fetchone()

    if not existing:
        # Ajouter la saison 2025-26
        cursor.execute("""
            INSERT INTO seasons (season_id, label, ligue1_rank, ucl_stage, top_scorer, goals_scored)
            VALUES (15, '2025-26', 1, 'En cours', 'En cours', 0)
        """)
        print("   ✅ Saison 2025-26 ajoutée")
        season_id = 15
    else:
        season_id = existing[0]
        print(f"   ℹ️  Saison 2025-26 déjà présente (id={season_id})")

    # Pour chaque joueur scrapé
    players_added = 0
    stats_added = 0

    for _, row in df_clean.iterrows():
        name = row.get('name', '')
        if not name:
            continue

        # Vérifier si le joueur existe
        cursor.execute("SELECT player_id FROM players WHERE name = ?", (name,))
        player = cursor.fetchone()

        if not player:
            # Calculer birth_year depuis l'âge
            age = row.get('age', 25)
            try:
                age = int(str(age).split('-')[0])
            except:
                age = 25
            birth_year = 2025 - age

            # Insérer le joueur
            cursor.execute("""
                INSERT INTO players (name, nationality, position, birth_year, foot)
                VALUES (?, ?, ?, ?, 'D')
            """, (name, row.get('nationality', 'N/A'), row.get('position', 'MID'), birth_year))
            player_id = cursor.lastrowid
            players_added += 1
        else:
            player_id = player[0]

        # Supprimer les stats existantes pour cette saison
        cursor.execute("DELETE FROM player_stats WHERE player_id=? AND season_id=?",
                       (player_id, season_id))

        # Insérer les nouvelles stats
        cursor.execute("""
            INSERT INTO player_stats
                (player_id, season_id, appearances, goals, assists, xG, minutes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            player_id, season_id,
            int(row.get('appearances', 0)),
            int(row.get('goals', 0)),
            int(row.get('assists', 0)),
            float(row.get('xG', 0.0)),
            int(row.get('minutes', 0))
        ))
        stats_added += 1

    conn.commit()
    conn.close()

    print(f"   ✅ {players_added} nouveaux joueurs ajoutés")
    print(f"   ✅ {stats_added} fiches stats mises à jour")


# ════════════════════════════════════════════════════════════════
# LANCEMENT
# ════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 55)
    print("  PSG DATA HUB — Scraping FBref")
    print(f"  Lancé le : {datetime.now().strftime('%d/%m/%Y à %H:%M')}")
    print("=" * 55)

    # Étape 1 : Scraper les stats joueurs
    df_raw = scrape_psg_squad_stats()

    # Étape 2 : Scraper l'historique des saisons
    scrape_psg_season_history()

    # Étape 3 : Nettoyage
    if df_raw is not None:
        df_clean = clean_squad_stats(df_raw)

        # Étape 4 : Mise à jour SQLite
        update_sqlite(df_clean)

        print("\n" + "=" * 55)
        print("  ✅ Scraping terminé avec succès !")
        print("  👉 Lance maintenant : python scripts/generate_data.py")
        print("     pour regénérer le JSON du dashboard")
        print("=" * 55)
    else:
        print("\n" + "=" * 55)
        print("  ⚠️  Le scraping a échoué (FBref inaccessible ou structure changée)")
        print("  💡 Les données simulées restent en place")
        print("  👉 Réessaie dans quelques minutes")
        print("=" * 55)
