"""
Add ISO 3166-1 alpha-3 codes and modern successor state codes to the olympics database.

Two new columns added to noc_regions:
  - iso3: The ISO 3166-1 alpha-3 code for the entity (NULL for non-state entities)
  - modern_iso3: The modern successor state's ISO3 code (for rolling up historical entities)

Historical entities are mapped to their primary modern successor:
  - URS/EUN/ROC → RUS
  - GDR/FRG/SAA → DEU
  - TCH → CZE (Czech Republic inherited IOC seat)
  - YUG/SCG → SRB (Serbia inherited IOC seat)
  - etc.
"""
import sqlite3

conn = sqlite3.connect("/media/emil/8tb_ssd_3/projects/olympics/olympics.db")

# Complete mapping: NOC → (iso3, modern_iso3, region_fix)
# iso3 = actual ISO code (or None for non-state entities)
# modern_iso3 = modern successor ISO code (same as iso3 for current countries)
# region_fix = corrected country name (or None to keep existing)
MAPPING = {
    # Current countries where NOC == ISO3
    "AFG": ("AFG", "AFG", "Afghanistan"),
    "ALB": ("ALB", "ALB", "Albania"),
    "ARG": ("ARG", "ARG", "Argentina"),
    "ARM": ("ARM", "ARM", "Armenia"),
    "AUS": ("AUS", "AUS", "Australia"),
    "AUT": ("AUT", "AUT", "Austria"),
    "AZE": ("AZE", "AZE", "Azerbaijan"),
    "BDI": ("BDI", "BDI", "Burundi"),
    "BEL": ("BEL", "BEL", "Belgium"),
    "BEN": ("BEN", "BEN", "Benin"),
    "BIH": ("BIH", "BIH", "Bosnia and Herzegovina"),
    "BLR": ("BLR", "BLR", "Belarus"),
    "BOL": ("BOL", "BOL", "Bolivia"),
    "BRA": ("BRA", "BRA", "Brazil"),
    "CAN": ("CAN", "CAN", "Canada"),
    "CHN": ("CHN", "CHN", "China"),
    "COD": ("COD", "COD", "DR Congo"),
    "COL": ("COL", "COL", "Colombia"),
    "COM": ("COM", "COM", "Comoros"),
    "CPV": ("CPV", "CPV", "Cape Verde"),
    "CUB": ("CUB", "CUB", "Cuba"),
    "CYP": ("CYP", "CYP", "Cyprus"),
    "CZE": ("CZE", "CZE", "Czech Republic"),
    "DJI": ("DJI", "DJI", "Djibouti"),
    "DMA": ("DMA", "DMA", "Dominica"),
    "DOM": ("DOM", "DOM", "Dominican Republic"),
    "ECU": ("ECU", "ECU", "Ecuador"),
    "EGY": ("EGY", "EGY", "Egypt"),
    "ERI": ("ERI", "ERI", "Eritrea"),
    "EST": ("EST", "EST", "Estonia"),
    "ETH": ("ETH", "ETH", "Ethiopia"),
    "FIN": ("FIN", "FIN", "Finland"),
    "FJI": ("FJI", "FJI", "Fiji"),
    "FRA": ("FRA", "FRA", "France"),
    "FSM": ("FSM", "FSM", "Micronesia"),
    "GAB": ("GAB", "GAB", "Gabon"),
    "GBR": ("GBR", "GBR", "United Kingdom"),
    "GEO": ("GEO", "GEO", "Georgia"),
    "GHA": ("GHA", "GHA", "Ghana"),
    "HKG": ("HKG", "HKG", "Hong Kong"),
    "HUN": ("HUN", "HUN", "Hungary"),
    "IND": ("IND", "IND", "India"),
    "IRL": ("IRL", "IRL", "Ireland"),
    "IRQ": ("IRQ", "IRQ", "Iraq"),
    "ISL": ("ISL", "ISL", "Iceland"),
    "ISR": ("ISR", "ISR", "Israel"),
    "ITA": ("ITA", "ITA", "Italy"),
    "JAM": ("JAM", "JAM", "Jamaica"),
    "JOR": ("JOR", "JOR", "Jordan"),
    "JPN": ("JPN", "JPN", "Japan"),
    "KAZ": ("KAZ", "KAZ", "Kazakhstan"),
    "KEN": ("KEN", "KEN", "Kenya"),
    "KGZ": ("KGZ", "KGZ", "Kyrgyzstan"),
    "KIR": ("KIR", "KIR", "Kiribati"),
    "KOR": ("KOR", "KOR", "South Korea"),
    "KUW": ("KWT", "KWT", "Kuwait"),
    "LAO": ("LAO", "LAO", "Laos"),
    "LBR": ("LBR", "LBR", "Liberia"),
    "LCA": ("LCA", "LCA", "Saint Lucia"),
    "LIE": ("LIE", "LIE", "Liechtenstein"),
    "LTU": ("LTU", "LTU", "Lithuania"),
    "LUX": ("LUX", "LUX", "Luxembourg"),
    "MDA": ("MDA", "MDA", "Moldova"),
    "MDV": ("MDV", "MDV", "Maldives"),
    "MEX": ("MEX", "MEX", "Mexico"),
    "MHL": ("MHL", "MHL", "Marshall Islands"),
    "MKD": ("MKD", "MKD", "North Macedonia"),
    "MLI": ("MLI", "MLI", "Mali"),
    "MLT": ("MLT", "MLT", "Malta"),
    "MNE": ("MNE", "MNE", "Montenegro"),
    "MOZ": ("MOZ", "MOZ", "Mozambique"),
    "MYA": ("MMR", "MMR", "Myanmar"),
    "NAM": ("NAM", "NAM", "Namibia"),
    "NOR": ("NOR", "NOR", "Norway"),
    "NRU": ("NRU", "NRU", "Nauru"),
    "NZL": ("NZL", "NZL", "New Zealand"),
    "PAK": ("PAK", "PAK", "Pakistan"),
    "PAN": ("PAN", "PAN", "Panama"),
    "PER": ("PER", "PER", "Peru"),
    "PLW": ("PLW", "PLW", "Palau"),
    "PNG": ("PNG", "PNG", "Papua New Guinea"),
    "POL": ("POL", "POL", "Poland"),
    "PRK": ("PRK", "PRK", "North Korea"),
    "QAT": ("QAT", "QAT", "Qatar"),
    "ROU": ("ROU", "ROU", "Romania"),
    "RUS": ("RUS", "RUS", "Russia"),
    "RWA": ("RWA", "RWA", "Rwanda"),
    "SEN": ("SEN", "SEN", "Senegal"),
    "SEY": ("SYC", "SYC", "Seychelles"),
    "SLE": ("SLE", "SLE", "Sierra Leone"),
    "SMR": ("SMR", "SMR", "San Marino"),
    "SOM": ("SOM", "SOM", "Somalia"),
    "SRB": ("SRB", "SRB", "Serbia"),
    "SSD": ("SSD", "SSD", "South Sudan"),
    "STP": ("STP", "STP", "Sao Tome and Principe"),
    "SUR": ("SUR", "SUR", "Suriname"),
    "SVK": ("SVK", "SVK", "Slovakia"),
    "SWE": ("SWE", "SWE", "Sweden"),
    "SWZ": ("SWZ", "SWZ", "Eswatini"),
    "SYR": ("SYR", "SYR", "Syria"),
    "THA": ("THA", "THA", "Thailand"),
    "TJK": ("TJK", "TJK", "Tajikistan"),
    "TKM": ("TKM", "TKM", "Turkmenistan"),
    "TLS": ("TLS", "TLS", "Timor-Leste"),
    "TTO": ("TTO", "TTO", "Trinidad and Tobago"),
    "TUN": ("TUN", "TUN", "Tunisia"),
    "TUR": ("TUR", "TUR", "Turkey"),
    "TUV": ("TUV", "TUV", "Tuvalu"),
    "UGA": ("UGA", "UGA", "Uganda"),
    "UKR": ("UKR", "UKR", "Ukraine"),
    "USA": ("USA", "USA", "United States"),
    "UZB": ("UZB", "UZB", "Uzbekistan"),
    "VEN": ("VEN", "VEN", "Venezuela"),
    "YEM": ("YEM", "YEM", "Yemen"),

    # Current countries where NOC ≠ ISO3
    "ALG": ("DZA", "DZA", "Algeria"),
    "ANG": ("AGO", "AGO", "Angola"),
    "ANT": ("ATG", "ATG", "Antigua and Barbuda"),
    "ARU": ("ABW", "ABW", "Aruba"),
    "ASA": ("ASM", "ASM", "American Samoa"),
    "BAH": ("BHS", "BHS", "Bahamas"),
    "BAN": ("BGD", "BGD", "Bangladesh"),
    "BAR": ("BRB", "BRB", "Barbados"),
    "BER": ("BMU", "BMU", "Bermuda"),
    "BHU": ("BTN", "BTN", "Bhutan"),
    "BIZ": ("BLZ", "BLZ", "Belize"),
    "BOT": ("BWA", "BWA", "Botswana"),
    "BRN": ("BHR", "BHR", "Bahrain"),       # IOC BRN = Bahrain (ISO BHR)
    "BRU": ("BRN", "BRN", "Brunei"),         # IOC BRU = Brunei (ISO BRN)
    "BUL": ("BGR", "BGR", "Bulgaria"),
    "BUR": ("BFA", "BFA", "Burkina Faso"),
    "CAF": ("CAF", "CAF", "Central African Republic"),
    "CAM": ("KHM", "KHM", "Cambodia"),
    "CAY": ("CYM", "CYM", "Cayman Islands"),
    "CGO": ("COG", "COG", "Congo"),
    "CHA": ("TCD", "TCD", "Chad"),
    "CHI": ("CHL", "CHL", "Chile"),
    "CIV": ("CIV", "CIV", "Ivory Coast"),
    "CMR": ("CMR", "CMR", "Cameroon"),
    "COK": ("COK", "COK", "Cook Islands"),
    "CRC": ("CRI", "CRI", "Costa Rica"),
    "CRO": ("HRV", "HRV", "Croatia"),
    "DEN": ("DNK", "DNK", "Denmark"),
    "ESA": ("SLV", "SLV", "El Salvador"),
    "ESP": ("ESP", "ESP", "Spain"),
    "FIJ": ("FJI", "FJI", "Fiji"),
    "GAM": ("GMB", "GMB", "Gambia"),
    "GBS": ("GNB", "GNB", "Guinea-Bissau"),
    "GEQ": ("GNQ", "GNQ", "Equatorial Guinea"),
    "GER": ("DEU", "DEU", "Germany"),
    "GRE": ("GRC", "GRC", "Greece"),
    "GRN": ("GRD", "GRD", "Grenada"),
    "GUA": ("GTM", "GTM", "Guatemala"),
    "GUI": ("GIN", "GIN", "Guinea"),
    "GUM": ("GUM", "GUM", "Guam"),
    "GUY": ("GUY", "GUY", "Guyana"),
    "HAI": ("HTI", "HTI", "Haiti"),
    "HON": ("HND", "HND", "Honduras"),
    "INA": ("IDN", "IDN", "Indonesia"),
    "IRI": ("IRN", "IRN", "Iran"),
    "ISV": ("VIR", "VIR", "US Virgin Islands"),
    "IVB": ("VGB", "VGB", "British Virgin Islands"),
    "KOS": ("XKX", "XKX", "Kosovo"),          # User-assigned code (not in ISO standard)
    "KSA": ("SAU", "SAU", "Saudi Arabia"),
    "LAT": ("LVA", "LVA", "Latvia"),
    "LBA": ("LBY", "LBY", "Libya"),
    "LBN": ("LBN", "LBN", "Lebanon"),
    "LES": ("LSO", "LSO", "Lesotho"),
    "LIB": ("LBN", "LBN", "Lebanon"),         # Historical IOC code for Lebanon
    "MAD": ("MDG", "MDG", "Madagascar"),
    "MAL": ("MYS", "MYS", "Malaysia"),         # Historical IOC code for Malaysia
    "MAR": ("MAR", "MAR", "Morocco"),
    "MAS": ("MYS", "MYS", "Malaysia"),
    "MAW": ("MWI", "MWI", "Malawi"),
    "MGL": ("MNG", "MNG", "Mongolia"),
    "MON": ("MCO", "MCO", "Monaco"),
    "MRI": ("MUS", "MUS", "Mauritius"),
    "MTN": ("MRT", "MRT", "Mauritania"),
    "NCA": ("NIC", "NIC", "Nicaragua"),
    "NED": ("NLD", "NLD", "Netherlands"),
    "NEP": ("NPL", "NPL", "Nepal"),
    "NGR": ("NGA", "NGA", "Nigeria"),
    "NIG": ("NER", "NER", "Niger"),
    "OMA": ("OMN", "OMN", "Oman"),
    "PAR": ("PRY", "PRY", "Paraguay"),
    "PHI": ("PHL", "PHL", "Philippines"),
    "PLE": ("PSE", "PSE", "Palestine"),
    "POR": ("PRT", "PRT", "Portugal"),
    "PUR": ("PRI", "PRI", "Puerto Rico"),
    "RSA": ("ZAF", "ZAF", "South Africa"),
    "SAM": ("WSM", "WSM", "Samoa"),
    "SGP": ("SGP", "SGP", "Singapore"),
    "SIN": ("SGP", "SGP", "Singapore"),        # Historical IOC code
    "SKN": ("KNA", "KNA", "Saint Kitts and Nevis"),
    "SLO": ("SVN", "SVN", "Slovenia"),
    "SOL": ("SLB", "SLB", "Solomon Islands"),
    "SRI": ("LKA", "LKA", "Sri Lanka"),
    "SUD": ("SDN", "SDN", "Sudan"),
    "SUI": ("CHE", "CHE", "Switzerland"),
    "TAN": ("TZA", "TZA", "Tanzania"),
    "TGA": ("TON", "TON", "Tonga"),
    "TOG": ("TGO", "TGO", "Togo"),
    "TPE": ("TWN", "TWN", "Taiwan"),
    "UAE": ("ARE", "ARE", "United Arab Emirates"),
    "URU": ("URY", "URY", "Uruguay"),
    "VAN": ("VUT", "VUT", "Vanuatu"),
    "VIE": ("VNM", "VNM", "Vietnam"),
    "VIN": ("VCT", "VCT", "Saint Vincent and the Grenadines"),
    "VNM": ("VNM", "VNM", "Vietnam"),          # Alternative code used in some records
    "ZAM": ("ZMB", "ZMB", "Zambia"),
    "ZIM": ("ZWE", "ZWE", "Zimbabwe"),

    # ====================================================================
    # HISTORICAL / DEFUNCT ENTITIES
    # iso3 = historical ISO code or invented pseudo-code
    # modern_iso3 = primary modern successor state
    # ====================================================================

    # --- Germany family ---
    "GDR": ("DDR", "DEU", "East Germany"),      # DDR not official ISO but widely recognized
    "FRG": ("DEU", "DEU", "West Germany"),       # FRG used ISO DEU at the time
    "SAA": ("DEU", "DEU", "Saarland"),           # Saar protectorate, now part of Germany

    # --- Soviet Union family ---
    "URS": ("SUN", "RUS", "Soviet Union"),       # SUN is the ISO reserved code for USSR
    "EUN": ("SUN", "RUS", "Unified Team"),       # 1992 post-Soviet transition team
    "ROC": ("RUS", "RUS", "ROC (Russia)"),       # Russian Olympic Committee 2020-2022

    # --- Czechoslovakia ---
    "TCH": ("CSK", "CZE", "Czechoslovakia"),     # CSK is the ISO reserved code for Czechoslovakia

    # --- Yugoslavia family ---
    "YUG": ("YUG", "SRB", "Yugoslavia"),         # YUG is the ISO reserved code
    "SCG": ("SCG", "SRB", "Serbia and Montenegro"),  # SCG was the ISO code 2003-2006

    # --- Other historical ---
    "AHO": ("ANT", "CUW", "Netherlands Antilles"),  # ANT was ISO code; Curacao (CUW) is successor NOC
    "ANZ": ("AUS", "AUS", "Australasia"),         # AUS+NZL combined 1908-1912
    "BOH": ("CZE", "CZE", "Bohemia"),            # Historical, now Czech Republic
    "COR": ("KOR", "KOR", "Korea (Unified)"),     # Unified Korean team
    "CRT": ("GRC", "GRC", "Crete"),              # Part of Greece
    "NBO": ("MYS", "MYS", "North Borneo"),       # Now part of Malaysia
    "NFL": ("CAN", "CAN", "Newfoundland"),        # Now part of Canada
    "RHO": ("RHO", "ZWE", "Rhodesia"),           # Now Zimbabwe
    "UAR": ("EGY", "EGY", "United Arab Republic"),  # Egypt+Syria union, Egypt was primary
    "WIF": ("JAM", "JAM", "West Indies Federation"),  # BWI 1960
    "BWI": ("JAM", "JAM", "British West Indies"),
    "YAR": ("YEM", "YEM", "North Yemen"),
    "YMD": ("YEM", "YEM", "South Yemen"),
    "SER": ("SRB", "SRB", "Serbia"),             # Transitional code

    # --- Non-state entities (no ISO3) ---
    "AIN": (None, None, "Individual Neutral Athletes"),
    "EOR": (None, None, "Refugee Olympic Team"),
    "IOA": (None, None, "Individual Olympic Athletes"),
    "ROT": (None, None, "Refugee Olympic Team"),
    "UNK": (None, None, "Unknown"),
}

# Add columns
try:
    conn.execute("ALTER TABLE noc_regions ADD COLUMN iso3 TEXT")
except:
    pass  # already exists
try:
    conn.execute("ALTER TABLE noc_regions ADD COLUMN modern_iso3 TEXT")
except:
    pass  # already exists

# Ensure all NOC codes exist in noc_regions
existing = set(r[0] for r in conn.execute("SELECT noc FROM noc_regions"))
all_nocs = set(r[0] for r in conn.execute("SELECT DISTINCT noc FROM results WHERE noc IS NOT NULL"))

for noc in all_nocs - existing:
    if noc in MAPPING:
        conn.execute("INSERT INTO noc_regions (noc, region, notes, iso3, modern_iso3) VALUES (?, ?, ?, ?, ?)",
                     (noc, MAPPING[noc][2], "", MAPPING[noc][0], MAPPING[noc][1]))
        print(f"  Added missing NOC: {noc} -> {MAPPING[noc]}")

# Update all rows
updated = 0
unmapped = []
for row in conn.execute("SELECT noc FROM noc_regions"):
    noc = row[0]
    if noc in MAPPING:
        iso3, modern, region = MAPPING[noc]
        conn.execute("UPDATE noc_regions SET iso3 = ?, modern_iso3 = ?, region = ? WHERE noc = ?",
                     (iso3, modern, region, noc))
        updated += 1
    else:
        unmapped.append(noc)

conn.commit()

print(f"\nUpdated {updated} NOC codes with ISO3 mappings")
if unmapped:
    print(f"Unmapped NOC codes ({len(unmapped)}): {unmapped}")

# Verification
print("\n=== VERIFICATION ===")
print("\nCountries where NOC ≠ ISO3:")
rows = conn.execute("SELECT noc, iso3, modern_iso3, region FROM noc_regions WHERE iso3 != noc AND iso3 IS NOT NULL ORDER BY noc").fetchall()
for r in rows:
    mod = f" (modern: {r[2]})" if r[2] != r[1] else ""
    print(f"  {r[0]} -> {r[1]}{mod}  [{r[3]}]")

print(f"\nHistorical entities (modern_iso3 differs from iso3):")
rows = conn.execute("SELECT noc, iso3, modern_iso3, region FROM noc_regions WHERE modern_iso3 != iso3 AND iso3 IS NOT NULL ORDER BY noc").fetchall()
for r in rows:
    print(f"  {r[0]} ({r[3]}): iso3={r[1]}, modern={r[2]}")

print(f"\nNon-state entities (NULL iso3):")
rows = conn.execute("SELECT noc, region FROM noc_regions WHERE iso3 IS NULL ORDER BY noc").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

print(f"\nNULL iso3 count: {conn.execute('SELECT COUNT(*) FROM noc_regions WHERE iso3 IS NULL').fetchone()[0]}")
print(f"NULL modern_iso3 count: {conn.execute('SELECT COUNT(*) FROM noc_regions WHERE modern_iso3 IS NULL').fetchone()[0]}")

conn.close()
