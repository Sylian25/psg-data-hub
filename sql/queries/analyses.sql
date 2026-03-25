-- ============================================================
-- PSG DATA HUB — Requêtes d'analyse SQL
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. TOP 10 BUTEURS ALL-TIME (ère QSI)
-- Compétences : JOIN, GROUP BY, ORDER BY, LIMIT
-- ────────────────────────────────────────────────────────────
SELECT
    p.name,
    SUM(ps.goals)   AS total_buts,
    SUM(ps.assists) AS total_passes_d,
    ROUND(SUM(ps.xG), 1) AS total_xG,
    ROUND(SUM(ps.goals) * 1.0 / NULLIF(SUM(ps.xG), 0), 2) AS ratio_buts_xG
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
GROUP BY p.name
ORDER BY total_buts DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 2. BILAN MERCATO NET PAR SAISON
-- Compétences : JOIN, CASE WHEN, agrégations conditionnelles
-- ────────────────────────────────────────────────────────────
SELECT
    s.label                                                             AS saison,
    ROUND(SUM(CASE WHEN t.direction = 'IN'  THEN t.fee_m_eur ELSE 0 END), 1) AS depenses_M,
    ROUND(SUM(CASE WHEN t.direction = 'OUT' THEN t.fee_m_eur ELSE 0 END), 1) AS recettes_M,
    ROUND(SUM(CASE WHEN t.direction = 'OUT' THEN t.fee_m_eur
                   WHEN t.direction = 'IN'  THEN -t.fee_m_eur ELSE 0 END), 1) AS bilan_net_M
FROM transfers t
JOIN seasons s ON t.season_id = s.season_id
GROUP BY s.label, s.season_id
ORDER BY s.season_id;


-- ────────────────────────────────────────────────────────────
-- 3. JOUEURS AVEC xG > BUTS (inefficacité devant le but)
-- Compétences : JOIN, WHERE, ROUND, filtrage sur minutes
-- ────────────────────────────────────────────────────────────
SELECT
    p.name,
    s.label                         AS saison,
    ps.goals                        AS buts,
    ps.xG                           AS expected_goals,
    ROUND(ps.xG - ps.goals, 2)      AS xG_manqués,
    ps.minutes
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
JOIN seasons  s ON ps.season_id = s.season_id
WHERE ps.xG > ps.goals
  AND ps.minutes > 500
ORDER BY xG_manqués DESC;


-- ────────────────────────────────────────────────────────────
-- 4. TRANSFERTS LES PLUS CHERS (top 10 entrants)
-- Compétences : JOIN, ORDER BY, LIMIT, filtrage direction
-- ────────────────────────────────────────────────────────────
SELECT
    p.name,
    s.label        AS saison,
    t.from_club,
    t.fee_m_eur    AS montant_M_EUR,
    t.transfer_type
FROM transfers t
JOIN players p ON t.player_id = p.player_id
JOIN seasons s ON t.season_id = s.season_id
WHERE t.direction = 'IN'
  AND t.fee_m_eur > 0
ORDER BY t.fee_m_eur DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────
-- 5. NATIONALITÉS DES JOUEURS RECRUTÉS
-- Compétences : JOIN, GROUP BY, COUNT, ORDER BY
-- ────────────────────────────────────────────────────────────
SELECT
    p.nationality,
    COUNT(DISTINCT t.player_id)  AS joueurs_recrutés,
    ROUND(SUM(t.fee_m_eur), 1)   AS total_investi_M
FROM transfers t
JOIN players p ON t.player_id = p.player_id
WHERE t.direction = 'IN'
GROUP BY p.nationality
ORDER BY joueurs_recrutés DESC;


-- ────────────────────────────────────────────────────────────
-- 6. ÉVOLUTION MBAPPÉ SAISON PAR SAISON
-- Compétences : JOIN, WHERE, ORDER BY
-- ────────────────────────────────────────────────────────────
SELECT
    s.label,
    ps.appearances,
    ps.goals,
    ps.assists,
    ps.xG,
    ps.minutes,
    ROUND(ps.goals * 90.0 / NULLIF(ps.minutes, 0), 2) AS buts_per_90
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
JOIN seasons s  ON ps.season_id = s.season_id
WHERE p.name = 'Kylian Mbappé'
ORDER BY s.season_id;


-- ────────────────────────────────────────────────────────────
-- 7. PALMARÈS COMPLET PAR COMPÉTITION
-- Compétences : GROUP BY, SUM, HAVING
-- ────────────────────────────────────────────────────────────
SELECT
    competition,
    COUNT(*)       AS saisons_jouées,
    SUM(won)       AS titres,
    COUNT(*) - SUM(won) AS finales_perdues
FROM trophies
GROUP BY competition
ORDER BY titres DESC;


-- ────────────────────────────────────────────────────────────
-- 8. SAISONS AVEC LE MEILLEUR RATIO ATTAQUE (buts marqués vs rang L1)
-- Compétences : calculs dans SELECT, sous-requête
-- ────────────────────────────────────────────────────────────
SELECT
    label,
    goals_scored,
    ligue1_rank,
    ucl_stage,
    CASE
        WHEN ligue1_rank = 1 THEN 'Champion'
        WHEN ligue1_rank = 2 THEN 'Vice-champion'
        ELSE 'Autre'
    END AS statut_ligue
FROM seasons
ORDER BY goals_scored DESC;


-- ────────────────────────────────────────────────────────────
-- 9. JOUEURS AYANT JOUÉ LE PLUS DE SAISONS AU PSG
-- Compétences : COUNT DISTINCT, GROUP BY, sous-requête
-- ────────────────────────────────────────────────────────────
SELECT
    p.name,
    p.nationality,
    p.position,
    COUNT(DISTINCT ps.season_id) AS nb_saisons,
    SUM(ps.goals)                AS total_buts,
    SUM(ps.appearances)          AS total_matchs
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
GROUP BY p.name, p.nationality, p.position
HAVING nb_saisons >= 3
ORDER BY nb_saisons DESC, total_buts DESC;


-- ────────────────────────────────────────────────────────────
-- 10. ANALYSE POSTE : moyenne des stats par position
-- Compétences : GROUP BY position, AVG, ROUND
-- ────────────────────────────────────────────────────────────
SELECT
    p.position,
    COUNT(DISTINCT p.player_id)           AS nb_joueurs,
    ROUND(AVG(ps.goals), 1)               AS moy_buts,
    ROUND(AVG(ps.assists), 1)             AS moy_passes_d,
    ROUND(AVG(ps.appearances), 1)         AS moy_matchs,
    ROUND(AVG(ps.minutes), 0)             AS moy_minutes
FROM player_stats ps
JOIN players p ON ps.player_id = p.player_id
GROUP BY p.position
ORDER BY moy_buts DESC;
