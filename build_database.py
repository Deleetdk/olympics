"""
Build the Olympics SQLite database from raw data sources.

Sources:
- KeithGalli/Olympics-Dataset (Olympedia.org): 1896-2022, all participants
- maciejbentkowski/olympics-data: Paris 2024 medalists

Output: olympics.db
"""
import sqlite3
import pandas as pd
import numpy as np
import os

DB_PATH = "olympics.db"
GALLI_RESULTS = "raw_data_galli/clean-data/results.csv"
GALLI_BIOS = "raw_data_galli/clean-data/bios.csv"
GALLI_NOC = "raw_data_galli/clean-data/noc_regions.csv"
GALLI_POP = "raw_data_galli/clean-data/populations.csv"
PARIS_2024 = "raw_data_2024/medallists.csv"

os.chdir("/media/emil/8tb_ssd_3/projects/olympics")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA foreign_keys=ON")

# ============================================================
# 1. NOC regions lookup
# ============================================================
print("Loading NOC regions...")
noc = pd.read_csv(GALLI_NOC)
noc.columns = ["noc", "region", "notes"]
noc.to_sql("noc_regions", conn, index=False, if_exists="replace")
print(f"  {len(noc)} NOC codes loaded")

# ============================================================
# 2. Athlete biographies
# ============================================================
print("Loading athlete bios...")
bios = pd.read_csv(GALLI_BIOS)
bios.columns = [
    "athlete_id", "name", "born_date", "born_city", "born_region",
    "born_country", "noc", "height_cm", "weight_kg", "died_date"
]
# Clean types
bios["athlete_id"] = bios["athlete_id"].astype(int)
bios["height_cm"] = pd.to_numeric(bios["height_cm"], errors="coerce")
bios["weight_kg"] = pd.to_numeric(bios["weight_kg"], errors="coerce")

bios.to_sql("athletes", conn, index=False, if_exists="replace")
print(f"  {len(bios)} athletes loaded")

# ============================================================
# 3. Results (1896-2022) — all participants
# ============================================================
print("Loading results (1896-2022)...")
results = pd.read_csv(GALLI_RESULTS)
results.columns = [
    "year", "season", "discipline", "event", "as_name",
    "athlete_id", "noc", "team", "place", "tied", "medal"
]
results["year"] = pd.to_numeric(results["year"], errors="coerce").astype("Int64")
results["athlete_id"] = pd.to_numeric(results["athlete_id"], errors="coerce").astype("Int64")
results["place"] = pd.to_numeric(results["place"], errors="coerce")

# Determine the city for each games
games_cities = {
    1896: "Athens", 1900: "Paris", 1904: "St. Louis", 1908: "London",
    1912: "Stockholm", 1920: "Antwerp", 1924: "Paris", 1928: "Amsterdam",
    1932: "Los Angeles", 1936: "Berlin", 1948: "London", 1952: "Helsinki",
    1956: "Melbourne", 1960: "Rome", 1964: "Tokyo", 1968: "Mexico City",
    1972: "Munich", 1976: "Montreal", 1980: "Moscow", 1984: "Los Angeles",
    1988: "Seoul", 1992: "Barcelona", 1994: "Lillehammer", 1996: "Atlanta",
    1998: "Nagano", 2000: "Sydney", 2002: "Salt Lake City", 2004: "Athens",
    2006: "Turin", 2008: "Beijing", 2010: "Vancouver", 2012: "London",
    2014: "Sochi", 2016: "Rio de Janeiro", 2018: "Pyeongchang",
    2020: "Tokyo", 2022: "Beijing", 2024: "Paris"
}
results["city"] = results["year"].map(lambda y: games_cities.get(y, None) if pd.notna(y) else None)

results.to_sql("results", conn, index=False, if_exists="replace")
print(f"  {len(results)} result rows loaded")
print(f"  Medalists: {results['medal'].notna().sum()}")
print(f"  Years: {results['year'].min()} - {results['year'].max()}")

# ============================================================
# 4. Paris 2024 medalists
# ============================================================
print("Loading Paris 2024 medalists...")
p24 = pd.read_csv(PARIS_2024)

# Find the max athlete_id already used, for generating IDs for new athletes
max_id = bios["athlete_id"].max()
print(f"  Max existing athlete_id: {max_id}")

# Normalize medal type
medal_map = {"Gold Medal": "Gold", "Silver Medal": "Silver", "Bronze Medal": "Bronze"}
p24["medal"] = p24["medal_type"].map(medal_map)

# Build the 2024 results
paris_results = pd.DataFrame({
    "year": 2024,
    "season": "Summer",
    "discipline": p24["discipline"],
    "event": p24["event"],
    "as_name": p24["name"],
    "athlete_id": pd.NA,  # will fill below
    "noc": p24["country_code"],
    "team": p24["team"].fillna(""),
    "place": p24["medal_code"],
    "tied": False,
    "medal": p24["medal"],
    "city": "Paris"
})

# Add 2024 athletes to bios table — assign new IDs
new_athletes = p24[["name", "birth_date", "country_code", "nationality_code", "code_athlete"]].drop_duplicates(subset=["code_athlete"])
new_athletes = new_athletes[new_athletes["code_athlete"].notna()]

athlete_id_map = {}
next_id = max_id + 1
for _, row in new_athletes.iterrows():
    aid = int(row["code_athlete"]) if pd.notna(row["code_athlete"]) else next_id
    # Use negative offset to avoid collisions with olympedia IDs
    new_id = 10_000_000 + next_id
    athlete_id_map[row["code_athlete"]] = new_id
    next_id += 1

new_bios = pd.DataFrame({
    "athlete_id": [athlete_id_map.get(code, None) for code in new_athletes["code_athlete"]],
    "name": new_athletes["name"].values,
    "born_date": new_athletes["birth_date"].values,
    "born_city": None,
    "born_region": None,
    "born_country": new_athletes["nationality_code"].values,
    "noc": new_athletes["country_code"].values,
    "height_cm": None,
    "weight_kg": None,
    "died_date": None,
})

new_bios.to_sql("athletes", conn, index=False, if_exists="append")
print(f"  {len(new_bios)} Paris 2024 athletes added to bios")

# Map athlete IDs back to results
code_to_id = dict(zip(p24["code_athlete"], [athlete_id_map.get(c) for c in p24["code_athlete"]]))
paris_results["athlete_id"] = p24["code_athlete"].map(code_to_id).astype("Int64")

paris_results.to_sql("results", conn, index=False, if_exists="append")
print(f"  {len(paris_results)} Paris 2024 result rows added")

# ============================================================
# 5. Population data (for per-capita analyses)
# ============================================================
print("Loading population data...")
pop = pd.read_csv(GALLI_POP)
# Melt wide format to long
id_cols = ["Country Name", "Country Code"]
year_cols = [c for c in pop.columns if c not in id_cols]
pop_long = pop.melt(id_vars=id_cols, value_vars=year_cols, var_name="year", value_name="population")
pop_long.columns = ["country_name", "country_code", "year", "population"]
pop_long["year"] = pd.to_numeric(pop_long["year"], errors="coerce").astype("Int64")
pop_long["population"] = pd.to_numeric(pop_long["population"], errors="coerce")
pop_long = pop_long.dropna(subset=["population"])
pop_long.to_sql("populations", conn, index=False, if_exists="replace")
print(f"  {len(pop_long)} population rows loaded")

# ============================================================
# 6. Create a convenience medal-only view
# ============================================================
print("Creating convenience views and indexes...")

conn.execute("""
CREATE VIEW IF NOT EXISTS medalists AS
SELECT
    r.year,
    r.season,
    r.city,
    r.discipline,
    r.event,
    r.medal,
    r.noc,
    r.athlete_id,
    COALESCE(a.name, r.as_name) AS athlete_name,
    a.born_date,
    a.born_country,
    a.height_cm,
    a.weight_kg,
    n.region AS country_name
FROM results r
LEFT JOIN athletes a ON r.athlete_id = a.athlete_id
LEFT JOIN noc_regions n ON r.noc = n.noc
WHERE r.medal IS NOT NULL
""")

conn.execute("""
CREATE VIEW IF NOT EXISTS medal_tally AS
SELECT
    r.noc,
    COALESCE(n.region, r.noc) AS country,
    r.year,
    r.season,
    SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END) AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END) AS bronze,
    COUNT(*) AS total
FROM results r
LEFT JOIN noc_regions n ON r.noc = n.noc
WHERE r.medal IS NOT NULL
GROUP BY r.noc, r.year, r.season
""")

conn.execute("""
CREATE VIEW IF NOT EXISTS medal_tally_alltime AS
SELECT
    r.noc,
    COALESCE(n.region, r.noc) AS country,
    SUM(CASE WHEN r.medal = 'Gold' THEN 1 ELSE 0 END) AS gold,
    SUM(CASE WHEN r.medal = 'Silver' THEN 1 ELSE 0 END) AS silver,
    SUM(CASE WHEN r.medal = 'Bronze' THEN 1 ELSE 0 END) AS bronze,
    COUNT(*) AS total
FROM results r
LEFT JOIN noc_regions n ON r.noc = n.noc
WHERE r.medal IS NOT NULL
GROUP BY r.noc
ORDER BY gold DESC, silver DESC, bronze DESC
""")

# Indexes for fast querying
conn.execute("CREATE INDEX IF NOT EXISTS idx_results_year ON results(year)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_results_noc ON results(noc)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_results_medal ON results(medal)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_results_discipline ON results(discipline)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_athletes_id ON athletes(athlete_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_athletes_noc ON athletes(noc)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_pop_code_year ON populations(country_code, year)")

conn.commit()

# ============================================================
# 7. Print summary
# ============================================================
print("\n" + "=" * 60)
print("DATABASE SUMMARY")
print("=" * 60)

for table in ["athletes", "results", "noc_regions", "populations"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count:,} rows")

for view in ["medalists", "medal_tally", "medal_tally_alltime"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
    print(f"  {view} (view): {count:,} rows")

print(f"\nYear range: {conn.execute('SELECT MIN(year), MAX(year) FROM results').fetchone()}")
print(f"Unique NOCs: {conn.execute('SELECT COUNT(DISTINCT noc) FROM results').fetchone()[0]}")
print(f"Unique disciplines: {conn.execute('SELECT COUNT(DISTINCT discipline) FROM results').fetchone()[0]}")

print("\nAll-time medal tally (top 15):")
rows = conn.execute("SELECT * FROM medal_tally_alltime LIMIT 15").fetchall()
print(f"  {'NOC':<6} {'Country':<25} {'Gold':>6} {'Silver':>6} {'Bronze':>6} {'Total':>6}")
print(f"  {'-'*55}")
for r in rows:
    print(f"  {r[0]:<6} {r[1]:<25} {r[2]:>6} {r[3]:>6} {r[4]:>6} {r[5]:>6}")

conn.close()
print(f"\nDatabase written to: {os.path.abspath(DB_PATH)}")
print(f"Size: {os.path.getsize(DB_PATH) / 1024 / 1024:.1f} MB")
