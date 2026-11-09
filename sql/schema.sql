-- ============================================================
-- PSG DATA HUB — Schéma SQL
-- Base de données : SQLite (psg_data.db)
-- ============================================================

-- Saisons du PSG (ère QSI : 2011-2027, saison 2026-27 en cours)
CREATE TABLE IF NOT EXISTS seasons (
    season_id    INTEGER PRIMARY KEY,
    label        TEXT    NOT NULL,        -- ex: "2022-23"
    ligue1_rank  INTEGER,                 -- classement final Ligue 1
    ucl_stage    TEXT,                    -- stade LDC atteint
    coupe_de_france TEXT,                 -- parcours en Coupe de France
    coupe_de_la_ligue TEXT,               -- parcours en Coupe de la Ligue (supprimée après 2019-20)
    top_scorer   TEXT,                    -- meilleur buteur du club
    top_scorer_goals INTEGER,             -- ses buts, toutes compétitions
    goals_scored INTEGER,                 -- buts du club, toutes compétitions
    data_quality TEXT                     -- verifie / partiel
);

-- Joueurs ayant porté le maillot du PSG
CREATE TABLE IF NOT EXISTS players (
    player_id    INTEGER PRIMARY KEY,
    name         TEXT    NOT NULL,
    nationality  TEXT,
    position     TEXT CHECK(position IN ('GK','DEF','MID','ATT')),
    birth_year   INTEGER,
    foot         TEXT CHECK(foot IN ('D','G','D/G'))
);

-- Mercato : tous les transferts entrants et sortants
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id   INTEGER PRIMARY KEY,
    player_id     INTEGER REFERENCES players(player_id),
    season_id     INTEGER REFERENCES seasons(season_id),
    direction     TEXT CHECK(direction IN ('IN','OUT')),
    from_club     TEXT,
    to_club       TEXT,
    fee_m_eur     REAL DEFAULT 0,          -- montant en millions €
    transfer_type TEXT,                    -- "Achat", "Prêt", "Libre"
    data_quality  TEXT                     -- verifie / a_recouper
);

-- Statistiques individuelles par saison
CREATE TABLE IF NOT EXISTS player_stats (
    stat_id      INTEGER PRIMARY KEY,
    player_id    INTEGER REFERENCES players(player_id),
    season_id    INTEGER REFERENCES seasons(season_id),
    appearances  INTEGER DEFAULT 0,
    goals        INTEGER DEFAULT 0,
    assists      INTEGER DEFAULT 0,
    xG           REAL    DEFAULT 0,        -- expected goals
    minutes      INTEGER DEFAULT 0,
    data_quality TEXT                      -- verifie / estime / partiel (NULL = valeur inconnue)
);

-- Palmarès : compétitions disputées et gagnées
CREATE TABLE IF NOT EXISTS trophies (
    trophy_id    INTEGER PRIMARY KEY,
    season_id    INTEGER REFERENCES seasons(season_id),
    competition  TEXT,
    won          INTEGER DEFAULT 0,        -- 1 = titre, 0 = non
    data_quality TEXT
);
