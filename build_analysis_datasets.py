"""
Build analysis-ready medal count datasets from olympics.db.

Prerequisite: run build_database.py then update_iso3.py first.

Two country filters x three season splits = six medal tally CSV files:

  Stable borders (1896-2024, ~119 countries with consistent borders):
    data/stable_borders_summer.csv
    data/stable_borders_winter.csv
    data/stable_borders_combined.csv

  Post-1994 modern countries (~138 countries, keyed by modern_iso3):
    data/post1994_summer.csv
    data/post1994_winter.csv
    data/post1994_combined.csv

  Medal tally columns: iso3/modern_iso3, country, gold, silver, bronze, total

Plus participation datasets (one row per country per Games):

  data/stable_borders_participation.csv
  data/post1994_participation.csv

  Participation columns: iso3/modern_iso3, country, year, season

NOTE: 2024 data only contains medalists, so participation for Paris 2024
is undercounted (92 NOCs vs ~206 actual participants).
"""
import sqlite3
import csv
import os

os.chdir("/media/emil/8tb_ssd_3/projects/olympics")

conn = sqlite3.connect("olympics.db")

# ============================================================
# NOCs to drop for each dataset
# ============================================================

DROP_STABLE = [
    # Germany family
    'GER', 'GDR', 'FRG', 'SAA',
    # Soviet family (USSR + all successor states)
    'URS', 'EUN', 'ROC', 'RUS', 'UKR', 'BLR', 'GEO', 'ARM', 'AZE',
    'KAZ', 'UZB', 'TKM', 'TJK', 'KGZ', 'MDA', 'EST', 'LAT', 'LTU',
    # Czechoslovakia family
    'TCH', 'CZE', 'SVK',
    # Yugoslavia family
    'YUG', 'SCG', 'SRB', 'CRO', 'SLO', 'BIH', 'MKD', 'MNE', 'KOS',
    # Yemen
    'YAR', 'YMD', 'YEM',
    # Sudan
    'SUD', 'SSD',
    # Ethiopia/Eritrea
    'ETH', 'ERI',
    # Historical/defunct
    'BOH', 'ANZ', 'CRT', 'NBO', 'NFL', 'BWI', 'WIF', 'UAR', 'RHO',
    # Non-state
    'AIN', 'EOR', 'IOA', 'ROT', 'UNK',
]

DROP_POST1994 = ['SCG', 'AIN', 'EOR', 'IOA', 'ROT', 'UNK']

# Country name overrides for modern_iso3 codes that have multiple NOCs
COUNTRY_NAMES = {
    'DEU': 'Germany',
    'AUS': 'Australia',
    'RUS': 'Russia',
    'CZE': 'Czech Republic',
    'MYS': 'Malaysia',
    'GRC': 'Greece',
    'EGY': 'Egypt',
    'JAM': 'Jamaica',
    'CAN': 'Canada',
    'KOR': 'South Korea',
    'VNM': 'Vietnam',
    'SGP': 'Singapore',
    'YEM': 'Yemen',
    'LBN': 'Lebanon',
}

def sql_list(items):
    return ", ".join(f"'{x}'" for x in items)

case_lines = "\n".join(
    f"        WHEN '{iso3}' THEN '{name}'"
    for iso3, name in COUNTRY_NAMES.items()
)

# ============================================================
# Season filters: None = combined, else 'Summer' or 'Winter'
# ============================================================
SEASONS = {
    "summer": "AND r.season = 'Summer'",
    "winter": "AND r.season = 'Winter'",
    "combined": "",
}

os.makedirs("data", exist_ok=True)

# ============================================================
# Build all 6 datasets
# ============================================================
for season_label, season_clause in SEASONS.items():

    # --- Stable borders ---
    view_name = f"medal_tally_stable_borders_{season_label}"
    csv_name = f"data/stable_borders_{season_label}.csv"

    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.execute(f"""
    CREATE VIEW {view_name} AS
    SELECT
        n.iso3,
        COALESCE(n.region, r.noc) AS country,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold,
        SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END) AS silver,
        SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END) AS bronze,
        COUNT(*) AS total
    FROM results r
    LEFT JOIN noc_regions n ON r.noc = n.noc
    WHERE r.medal IS NOT NULL
      AND r.noc NOT IN ({sql_list(DROP_STABLE)})
      AND r.noc IS NOT NULL
      {season_clause}
    GROUP BY n.iso3
    ORDER BY gold DESC, silver DESC, bronze DESC
    """)

    rows = conn.execute(f"SELECT * FROM {view_name}").fetchall()
    with open(csv_name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["iso3", "country", "gold", "silver", "bronze", "total"])
        w.writerows(rows)
    print(f"  {csv_name}: {len(rows)} countries")

    # --- Post-1994 ---
    view_name = f"medal_tally_post1994_{season_label}"
    csv_name = f"data/post1994_{season_label}.csv"

    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.execute(f"""
    CREATE VIEW {view_name} AS
    SELECT
        n.modern_iso3,
        CASE n.modern_iso3
    {case_lines}
            ELSE COALESCE(n.region, r.noc)
        END AS country,
        SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold,
        SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END) AS silver,
        SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END) AS bronze,
        COUNT(*) AS total
    FROM results r
    LEFT JOIN noc_regions n ON r.noc = n.noc
    WHERE r.medal IS NOT NULL
      AND r.year >= 1994
      AND r.noc NOT IN ({sql_list(DROP_POST1994)})
      AND r.noc IS NOT NULL
      {season_clause}
    GROUP BY n.modern_iso3
    ORDER BY gold DESC, silver DESC, bronze DESC
    """)

    rows = conn.execute(f"SELECT * FROM {view_name}").fetchall()
    with open(csv_name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["modern_iso3", "country", "gold", "silver", "bronze", "total"])
        w.writerows(rows)
    print(f"  {csv_name}: {len(rows)} countries")

conn.commit()

# ============================================================
# Participation datasets (one row per country per Games)
# NOTE: 2024 only has medalists, so participation is undercounted
# ============================================================
print("\nBuilding participation datasets...")

# --- Stable borders ---
conn.execute("DROP VIEW IF EXISTS participation_stable_borders")
conn.execute(f"""
CREATE VIEW participation_stable_borders AS
SELECT DISTINCT
    n.iso3,
    COALESCE(n.region, r.noc) AS country,
    r.year,
    r.season
FROM results r
LEFT JOIN noc_regions n ON r.noc = n.noc
WHERE r.noc NOT IN ({sql_list(DROP_STABLE)})
  AND r.noc IS NOT NULL
  AND r.year IS NOT NULL
  AND r.season IS NOT NULL
ORDER BY r.year, r.season, n.iso3
""")

rows = conn.execute("SELECT * FROM participation_stable_borders").fetchall()
with open("data/stable_borders_participation.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["iso3", "country", "year", "season"])
    w.writerows(rows)
n_countries = len(set(r[0] for r in rows))
n_games = len(set((r[2], r[3]) for r in rows))
print(f"  data/stable_borders_participation.csv: {len(rows)} rows ({n_countries} countries, {n_games} Games)")

# --- Post-1994 ---
conn.execute("DROP VIEW IF EXISTS participation_post1994")
conn.execute(f"""
CREATE VIEW participation_post1994 AS
SELECT DISTINCT
    n.modern_iso3,
    CASE n.modern_iso3
{case_lines}
        ELSE COALESCE(n.region, r.noc)
    END AS country,
    r.year,
    r.season
FROM results r
LEFT JOIN noc_regions n ON r.noc = n.noc
WHERE r.year >= 1994
  AND r.noc NOT IN ({sql_list(DROP_POST1994)})
  AND r.noc IS NOT NULL
  AND r.year IS NOT NULL
  AND r.season IS NOT NULL
ORDER BY r.year, r.season, n.modern_iso3
""")

rows = conn.execute("SELECT * FROM participation_post1994").fetchall()
with open("data/post1994_participation.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["modern_iso3", "country", "year", "season"])
    w.writerows(rows)
n_countries = len(set(r[0] for r in rows))
n_games = len(set((r[2], r[3]) for r in rows))
print(f"  data/post1994_participation.csv: {len(rows)} rows ({n_countries} countries, {n_games} Games)")

conn.commit()

# ============================================================
# Drop old views that are now replaced
# ============================================================
for old in ["medal_tally_stable_borders", "medal_tally_post1994"]:
    conn.execute(f"DROP VIEW IF EXISTS {old}")
conn.commit()

# ============================================================
# Verification
# ============================================================
print("\n=== VERIFICATION ===")

# Summer + Winter should equal Combined for stable borders
s = conn.execute("SELECT SUM(total) FROM medal_tally_stable_borders_summer").fetchone()[0]
w = conn.execute("SELECT SUM(total) FROM medal_tally_stable_borders_winter").fetchone()[0]
c = conn.execute("SELECT SUM(total) FROM medal_tally_stable_borders_combined").fetchone()[0]
print(f"Stable borders: summer({s}) + winter({w}) = {s+w}, combined = {c}, match: {s+w == c}")

s = conn.execute("SELECT SUM(total) FROM medal_tally_post1994_summer").fetchone()[0]
w = conn.execute("SELECT SUM(total) FROM medal_tally_post1994_winter").fetchone()[0]
c = conn.execute("SELECT SUM(total) FROM medal_tally_post1994_combined").fetchone()[0]
print(f"Post-1994: summer({s}) + winter({w}) = {s+w}, combined = {c}, match: {s+w == c}")

# Spot check USA summer
usa_summer = conn.execute("SELECT total FROM medal_tally_stable_borders_summer WHERE iso3 = 'USA'").fetchone()[0]
usa_manual = conn.execute("SELECT COUNT(*) FROM results WHERE noc = 'USA' AND medal IS NOT NULL AND season = 'Summer'").fetchone()[0]
print(f"USA summer (stable): {usa_summer}, manual: {usa_manual}, match: {usa_summer == usa_manual}")

# Top 5 summer stable
print("\nStable borders summer top 5:")
for r in conn.execute("SELECT * FROM medal_tally_stable_borders_summer LIMIT 5").fetchall():
    print(f"  {r[0]:>5} {r[1]:<25} {r[2]:>5}G {r[3]:>5}S {r[4]:>5}B = {r[5]:>5}")

print("\nPost-1994 summer top 5:")
for r in conn.execute("SELECT * FROM medal_tally_post1994_summer LIMIT 5").fetchall():
    print(f"  {r[0]:>5} {r[1]:<25} {r[2]:>5}G {r[3]:>5}S {r[4]:>5}B = {r[5]:>5}")

# Clean up old CSV files
for old_file in ["data/analysis_stable_borders.csv", "data/analysis_post1994.csv"]:
    if os.path.exists(old_file):
        os.remove(old_file)
        print(f"Removed old file: {old_file}")

conn.close()
print("\nDone.")
