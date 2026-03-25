"""
PSG DATA HUB
Notebook 02 — Nettoyage, Exploration & Analyse
================================================
Exécuter après generate_data.py
"""

# %%
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings("ignore")

# ── Style PSG ────────────────────────────────────────────────
PSG_BLEU   = "#004170"
PSG_ROUGE  = "#DA291C"
PSG_OR     = "#C7A84B"
PSG_BLANC  = "#FFFFFF"
BG_DARK    = "#0D1117"
BG_CARD    = "#161B22"
TEXT_LIGHT = "#E6EDF3"
TEXT_MUTED = "#8B949E"

plt.rcParams.update({
    "figure.facecolor": BG_DARK,
    "axes.facecolor":   BG_CARD,
    "axes.edgecolor":   "#30363D",
    "axes.labelcolor":  TEXT_LIGHT,
    "xtick.color":      TEXT_MUTED,
    "ytick.color":      TEXT_MUTED,
    "text.color":       TEXT_LIGHT,
    "grid.color":       "#21262D",
    "grid.linestyle":   "--",
    "grid.alpha":       0.5,
    "font.family":      "DejaVu Sans",
    "axes.titlesize":   13,
    "axes.titleweight": "bold",
})

# ── Connexion ─────────────────────────────────────────────────
conn = sqlite3.connect("data/psg_data.db")


# %%
# ════════════════════════════════════════════════════
# 1. CHARGEMENT & INSPECTION DES DONNÉES
# ════════════════════════════════════════════════════
print("=" * 50)
print("1. CHARGEMENT DES DONNÉES")
print("=" * 50)

tables = ["seasons", "players", "transfers", "player_stats", "trophies"]
dfs = {}
for t in tables:
    dfs[t] = pd.read_sql(f"SELECT * FROM {t}", conn)
    print(f"\n📋 {t.upper()} — {len(dfs[t])} lignes × {len(dfs[t].columns)} colonnes")
    print(dfs[t].dtypes.to_string())

# %%
# ════════════════════════════════════════════════════
# 2. NETTOYAGE & QUALITÉ DES DONNÉES
# ════════════════════════════════════════════════════
print("\n" + "=" * 50)
print("2. AUDIT QUALITÉ")
print("=" * 50)

for name, df in dfs.items():
    nulls = df.isnull().sum().sum()
    dupes = df.duplicated().sum()
    print(f"  {name}: {nulls} valeurs nulles, {dupes} doublons")

# Vérification de cohérence
stats = dfs["player_stats"]
anomalies = stats[stats["xG"] < 0]
print(f"\n  xG négatifs détectés : {len(anomalies)}")

# Ajout colonne calculée : buts par 90 min
stats["buts_per_90"] = (stats["goals"] * 90 / stats["minutes"].replace(0, np.nan)).round(2)
print("  ✅ Colonne 'buts_per_90' ajoutée")

# Export nettoyé
stats.to_csv("data/processed/player_stats_clean.csv", index=False)
print("  ✅ Données nettoyées exportées")


# %%
# ════════════════════════════════════════════════════
# 3. VISUALISATION — TOP BUTEURS ALL-TIME
# ════════════════════════════════════════════════════
top_scorers = pd.read_sql("""
    SELECT p.name, SUM(ps.goals) AS total_buts,
           SUM(ps.assists) AS total_ast,
           ROUND(SUM(ps.xG),1) AS total_xG
    FROM player_stats ps JOIN players p ON ps.player_id=p.player_id
    GROUP BY p.name ORDER BY total_buts DESC LIMIT 10
""", conn)

fig, ax = plt.subplots(figsize=(12, 6))
fig.patch.set_facecolor(BG_DARK)

y = range(len(top_scorers))
bars_buts = ax.barh(y, top_scorers["total_buts"], color=PSG_BLEU, height=0.5, label="Buts", zorder=3)
bars_xg   = ax.barh(y, top_scorers["total_xG"],   color=PSG_OR,   height=0.5, alpha=0.5, label="xG", zorder=2)

ax.set_yticks(y)
ax.set_yticklabels(top_scorers["name"], fontsize=11)
ax.invert_yaxis()
ax.set_xlabel("Total (ère QSI)", fontsize=10)
ax.set_title("🔵 Top 10 Buteurs PSG — Ère QSI (2011-2025)", fontsize=14, pad=15, color=TEXT_LIGHT)
ax.legend(loc="lower right")
ax.grid(axis="x", zorder=0)

for bar, val in zip(bars_buts, top_scorers["total_buts"]):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
            f"{int(val)}", va="center", fontsize=10, color=PSG_OR, fontweight="bold")

plt.tight_layout()
plt.savefig("dashboard/assets/top_scorers.png", dpi=150, bbox_inches="tight", facecolor=BG_DARK)
print("✅ top_scorers.png exporté")
plt.close()


# %%
# ════════════════════════════════════════════════════
# 4. VISUALISATION — BILAN MERCATO PAR SAISON
# ════════════════════════════════════════════════════
mercato = pd.read_sql("""
    SELECT s.label,
        ROUND(SUM(CASE WHEN t.direction='IN'  THEN t.fee_m_eur ELSE 0 END),1) AS depenses,
        ROUND(SUM(CASE WHEN t.direction='OUT' THEN t.fee_m_eur ELSE 0 END),1) AS recettes
    FROM transfers t JOIN seasons s ON t.season_id=s.season_id
    GROUP BY s.label, s.season_id ORDER BY s.season_id
""", conn)
mercato["bilan"] = mercato["recettes"] - mercato["depenses"]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
fig.patch.set_facecolor(BG_DARK)

x = range(len(mercato))
width = 0.35

ax1.bar([i - width/2 for i in x], mercato["depenses"],  width, label="Dépenses",  color=PSG_ROUGE,  alpha=0.85)
ax1.bar([i + width/2 for i in x], mercato["recettes"],  width, label="Recettes",  color=PSG_BLEU,   alpha=0.85)
ax1.set_title("Dépenses vs Recettes Mercato (en M€)", fontsize=13, color=TEXT_LIGHT)
ax1.legend()
ax1.grid(axis="y", zorder=0)

colors_bilan = [PSG_BLEU if b >= 0 else PSG_ROUGE for b in mercato["bilan"]]
ax2.bar(x, mercato["bilan"], color=colors_bilan, alpha=0.85)
ax2.axhline(0, color=TEXT_MUTED, linewidth=1)
ax2.set_title("Bilan Net par Saison (en M€)", fontsize=13, color=TEXT_LIGHT)
ax2.set_xticks(list(x))
ax2.set_xticklabels(mercato["label"], rotation=45, ha="right", fontsize=9)
ax2.grid(axis="y", zorder=0)

plt.tight_layout()
plt.savefig("dashboard/assets/mercato.png", dpi=150, bbox_inches="tight", facecolor=BG_DARK)
print("✅ mercato.png exporté")
plt.close()


# %%
# ════════════════════════════════════════════════════
# 5. VISUALISATION — ÉVOLUTION MBAPPÉ
# ════════════════════════════════════════════════════
mbappe = pd.read_sql("""
    SELECT s.label, ps.goals, ps.assists, ps.xG, ps.minutes,
           ROUND(ps.goals*90.0/ps.minutes, 2) AS buts_per_90
    FROM player_stats ps
    JOIN players p ON ps.player_id=p.player_id
    JOIN seasons s  ON ps.season_id=s.season_id
    WHERE p.name='Kylian Mbappé' ORDER BY s.season_id
""", conn)

fig, ax = plt.subplots(figsize=(12, 5))
fig.patch.set_facecolor(BG_DARK)

x = range(len(mbappe))
ax.fill_between(x, mbappe["goals"],   alpha=0.3, color=PSG_BLEU)
ax.fill_between(x, mbappe["assists"], alpha=0.3, color=PSG_OR)
ax.plot(x, mbappe["goals"],   "o-", color=PSG_BLEU, linewidth=2.5, markersize=7, label="Buts")
ax.plot(x, mbappe["assists"], "s-", color=PSG_OR,   linewidth=2.5, markersize=7, label="Passes déc.")
ax.plot(x, mbappe["xG"],      "^--",color=PSG_ROUGE,linewidth=1.5, markersize=5, label="xG", alpha=0.7)

for i, row in mbappe.iterrows():
    idx = i - mbappe.index[0]
    ax.annotate(str(int(row["goals"])), (idx, row["goals"]+0.5),
                ha="center", fontsize=9, color=PSG_BLEU, fontweight="bold")

ax.set_xticks(list(x))
ax.set_xticklabels(mbappe["label"], rotation=30, ha="right")
ax.set_title("📈 Kylian Mbappé — Évolution au PSG (2018-2024)", fontsize=14, color=TEXT_LIGHT)
ax.legend()
ax.grid(axis="y")

plt.tight_layout()
plt.savefig("dashboard/assets/mbappe_evolution.png", dpi=150, bbox_inches="tight", facecolor=BG_DARK)
print("✅ mbappe_evolution.png exporté")
plt.close()


# %%
# ════════════════════════════════════════════════════
# 6. VISUALISATION — EFFECTIF ACTUEL (radar par joueur)
# ════════════════════════════════════════════════════
squad = pd.read_sql("""
    SELECT p.name, p.position, ps.goals, ps.assists, ps.appearances,
           ps.xG, ps.minutes
    FROM player_stats ps JOIN players p ON ps.player_id=p.player_id
    WHERE ps.season_id=14 ORDER BY ps.goals DESC
""", conn)

fig, ax = plt.subplots(figsize=(14, 6))
fig.patch.set_facecolor(BG_DARK)

pos_colors = {"ATT": PSG_ROUGE, "MID": PSG_BLEU, "DEF": PSG_OR, "GK": "#7B61FF"}
for _, row in squad.iterrows():
    color = pos_colors.get(row["position"], TEXT_MUTED)
    ax.scatter(row["appearances"], row["goals"], s=row["assists"]*80+50,
               color=color, alpha=0.75, edgecolors="white", linewidth=0.5, zorder=3)
    ax.annotate(row["name"].split()[-1], (row["appearances"], row["goals"]+0.2),
                ha="center", fontsize=7.5, color=TEXT_LIGHT)

legend_patches = [mpatches.Patch(color=v, label=k) for k, v in pos_colors.items()]
ax.legend(handles=legend_patches, loc="upper left", fontsize=9)
ax.set_xlabel("Matchs joués")
ax.set_ylabel("Buts")
ax.set_title("🔵 Effectif PSG 2024-25 — Buts × Apparitions (taille = passes déc.)",
             fontsize=13, color=TEXT_LIGHT)
ax.grid(zorder=0)

plt.tight_layout()
plt.savefig("dashboard/assets/squad_bubble.png", dpi=150, bbox_inches="tight", facecolor=BG_DARK)
print("✅ squad_bubble.png exporté")
plt.close()

conn.close()
print("\n✅ Tous les graphiques générés dans dashboard/assets/")
