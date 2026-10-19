"""CPOS lead feed engine (version 2.3): finds Canadian businesses likely to take card payments in person.

Runs on GitHub Actions. Each run:
  1. Knows where Canada's cities, towns and villages are, from Overture Maps' own list of places (the divisions
     data, rebuilt once per Overture release). No fixed city list: every locality in every province and territory.
  2. Plans which areas to search: the seed market (Ottawa) first, then the rest of the home province (Ontario), then
     the rest of Canada, by configurable shares, balanced across provinces and between big cities and small towns.
     It never searches the same area twice in a release unless there are still good businesses left there.
  3. Reads every business in those areas from Overture Maps. Every Overture business type is considered, not a fixed
     list: the category model (model/category_rules.json) estimates how likely each type is to take card payments
     face to face, and types it does not know yet are tried in small numbers.
  4. Scores every business (0 to 100) and says why, marking what is verified from the listing, what is an estimate
     and what was learned from results. Picks a batch: best first, but every viable type and region keeps a share.
  5. Learns from the outreach agent's results (state/feedback.json): business types and markets where emails are
     found and owners reply move up, the others move down, never to zero. Strong types lead to their related types.
  6. Skips businesses the agent already knows (state/known_keys.txt, hashed) or was given before (state/sent_ids.txt).
  7. Writes the batch (feed/history/), a run report (feed/report/latest.json) and the coverage so far
     (feed/coverage.json, feed/COVERAGE.md).

Settings: config.json (written by the agent from its Settings tab), or for a run by hand BATCH_SIZE and PROVINCES.
Map data: Overture Maps Foundation, places licensed CDLA Permissive 2.0, divisions ODbL.
"""
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sys
import time
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(HERE, "state")
FEED_DIR = os.path.join(HERE, "feed")
SENT_FILE = os.path.join(STATE_DIR, "sent_ids.txt")
KNOWN_FILE = os.path.join(STATE_DIR, "known_keys.txt")          # written by the agent: what it already has
SENT_KEYS_FILE = os.path.join(STATE_DIR, "sent_keys.txt")       # written here: websites and phones handed over before
FEEDBACK_FILE = os.path.join(STATE_DIR, "feedback.json")
MARKETS_FILE = os.path.join(STATE_DIR, "markets.json")
TYPES_FILE = os.path.join(STATE_DIR, "business_types.json")
RUNS_FILE = os.path.join(STATE_DIR, "runs.jsonl")
ERROR_FILE = os.path.join(STATE_DIR, "last_error.txt")
CONFIG_FILE = os.path.join(HERE, "config.json")
RULES_FILE = os.path.join(HERE, "model", "category_rules.json")

CANADA_BBOX = (-141.01, 41.67, -52.61, 83.12)
PROVINCES = {"ON": "Ontario", "QC": "Quebec", "BC": "British Columbia", "AB": "Alberta", "MB": "Manitoba", "SK": "Saskatchewan",
             "NS": "Nova Scotia", "NB": "New Brunswick", "NL": "Newfoundland and Labrador", "PE": "Prince Edward Island",
             "YT": "Yukon", "NT": "Northwest Territories", "NU": "Nunavut"}
PROVINCE_NAMES = dict((v.upper(), k) for k, v in PROVINCES.items())
PROVINCE_NAMES.update({"QUEBEC": "QC", "PEI": "PE", "NEWFOUNDLAND": "NL", "ONT": "ON", "B.C.": "BC", "N.W.T.": "NT"})
POSTAL_PROV = {"K": "ON", "L": "ON", "M": "ON", "N": "ON", "P": "ON", "G": "QC", "H": "QC", "J": "QC", "V": "BC", "T": "AB",
               "R": "MB", "S": "SK", "B": "NS", "E": "NB", "A": "NL", "C": "PE", "Y": "YT"}
CA_AREA_CODES = set("""204 226 236 249 250 263 289 306 343 354 365 367 368 382 403 416 418 428 431 437 438 450 468 474 506 514 519 548 579 581
584 587 604 613 639 647 672 683 705 709 742 753 778 780 782 807 819 825 867 873 879 902 905""".split())

DEFAULTS = {
    "batch_size": 300,                 # businesses handed to the agent per run
    "mix": {"seed": 0.3, "home": 0.3, "national": 0.4},   # share of each batch: the seed market, the rest of the home province, the rest of Canada
    "seed": {"name": "Ottawa", "lat": 45.4215, "lon": -75.6972, "radius_km": 25, "province": "ON"},
    "home_province": "ON",
    "include_provinces": [],           # empty: all of Canada
    "include_quebec": False,           # Quebec businesses are counted but not picked until outreach in French is set up
    "min_score": 25,                   # below this a business is counted but not handed over
    "min_fit": 20,                     # business types the model rates below this rarely take card payments in person
    "explore_share": 0.2,              # of each batch, for business types and regions not tested yet
    "explore_min_picks": 15,           # a business type counts as tested once this many were handed over
    "small_town_share": 0.3,           # of the home and national picks, from places under small_town_pop
    "small_town_pop": 30000,
    "max_minutes": 20,                 # time for searching per run (the job also needs a few minutes to start and save)
    "max_areas": 45,                   # areas searched per run at most
    "area_km": 6,                      # big cities are searched in squares this wide
    "per_area_cap": 40,                # picks from one area per run at most, so one neighbourhood is never drained at once
    "per_type_cap_share": 0.12,        # picks of one business type per batch at most (share of the batch)
    "min_type_weight": 0.2,            # every viable business type keeps at least this weight against the best one
    "min_confidence": 0.5,
    "pool_factor": 2.0,                # candidates gathered per pick, so there is choice across types
    "blocked": ["gambling", "casino", "adult", "cannabis", "dispensary", "firearms", "gun shop", "cryptocurrency", "tobacco", "vape",
                "payday", "pawn", "escort", "strip club"],
}

WANTED = ["id", "names", "basic_category", "taxonomy", "categories", "confidence", "websites", "socials", "emails", "phones",
          "brand", "addresses", "operating_status", "bbox"]
DIVISION_COLS = ["id", "names", "subtype", "country", "region", "population", "bbox"]
COLUMNS = ["Company", "Category", "Website", "Email", "Phone", "Address", "City", "Province", "Instagram", "Sector", "BusinessType",
           "Score", "ScoreReasons", "Terminal", "Market", "OvertureId"]

CHAIN_RE = re.compile(r"\b(speedy|midas|mr\.? ?lube|jiffy lube|canadian tire|kal tire|ok tire|active green|napa auto ?pro|firestone|goodyear|fountain tire|great canadian oil change|costco|walmart|tim hortons|mcdonald'?s|subway|starbucks|a&w|pizza pizza|domino'?s|little caesars|pizza hut|kfc|wendy'?s|burger king|popeyes|dairy queen|shoppers drug mart|rexall|jean coutu|uniprix|familiprix|proxim|london drugs|lawtons|dollarama|great clips|first choice haircutters|magicuts|chatters|supercuts|pet valu|petsmart|home depot|rona|lowe'?s|staples|best buy|the source|the brick|leon'?s|sleep country|lenscrafters|specsavers|h&r block|liberty tax|u-?haul|enterprise rent|budget rent|hertz|avis|jugo juice|booster juice|freshii|mary brown'?s|harvey'?s|swiss chalet|boston pizza|east side mario'?s|the keg|montana'?s|kelsey'?s|milestones|st\.? louis bar|second cup|country style|coffee time|mr\.? sub|quiznos|marble slab|cora|denny'?s|sobeys|loblaws|no frills|food basics|freshco|metro\b|save-on-foods|safeway|iga\b|giant tiger|circle k|couche-tard|7-eleven|mac'?s|petro-canada|esso|shell\b|ultramar|irving|pioneer|husky|winners|marshalls|homesense|sport chek|mark'?s|indigo|chapters|princess auto|home hardware|michaels|goodlife|anytime fitness|planet fitness|orangetheory|f45|snap fitness|fit4less|curves|sally beauty|lush\b|bell\b|rogers|telus|freedom mobile|fido|koodo|virgin plus|uber|lyft|best western|holiday inn|comfort inn|days inn|super 8|hampton inn|ramada|travelodge|motel 6|quality inn|marriott|hilton|sheraton|fairmont|delta hotels)\b", re.I)
IG_RE = re.compile(r"instagram\.com/(?:#!/)?@?([A-Za-z0-9._]{1,30})", re.I)
IG_RESERVED = re.compile(r"^(p|reel|reels|explore|stories|accounts|tv|direct|about|legal|developer|developers|web|share|instagram|login)$", re.I)
SOCIAL_RE = re.compile(r"(google\.|goo\.gl|g\.page|facebook\.|fb\.com|instagram\.|yelp\.|tripadvisor\.|linkedin\.|twitter\.|x\.com|tiktok\.|youtube\.|ubereats\.|doordash\.|skipthedishes\.|opentable\.|linktr\.ee|yellowpages\.|canpages\.|411\.ca|booksy\.|fresha\.|vagaro\.|square\.site|wa\.me)", re.I)
STREET_RE = re.compile(r"^\s*\d+[a-z]?\b|\b\d+\s+\w+.*\b(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|way|cres|crescent|lane|ln|ct|court|pl|place|hwy|highway|pkwy|parkway|unit|suite|rue|chemin|route|sideroad|concession|line)\b", re.I)
MOBILE_RE = re.compile(r"\bmobile\b(?! (phone|home))|\bon ?site\b|\bwe come to you\b", re.I)


# ============================== small helpers ==============================
def norm(t):
    t = unicodedata.normalize("NFKD", str(t or "")).encode("ascii", "ignore").decode().lower().replace("&", " and ")
    return re.sub(r"\s+", " ", re.sub(r"[_/\-]+", " ", t)).strip()


def first(v):
    """First non-empty item of a list (or the value itself)."""
    if isinstance(v, (list, tuple)):
        for x in v:
            if x:
                return x.get("value") or x.get("url") if isinstance(x, dict) else x
        return ""
    return v or ""


def phone_digits(p):
    d = re.sub(r"\D", "", str(p or ""))
    return d[1:] if len(d) == 11 and d[0] == "1" else d


def phone_fmt(p):
    d = phone_digits(p)
    return "(%s) %s-%s" % (d[:3], d[3:6], d[6:]) if len(d) == 10 else ""


def domain_of(u):
    u = str(u or "").strip()
    if not u or SOCIAL_RE.search(u):
        return ""
    m = re.sub(r"^https?://", "", u, flags=re.I).split("/")[0].split("?")[0].split("#")[0].lower()
    m = re.sub(r"^www\.", "", m)
    return m if re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", m) else ""


def key_hash(k):
    """Same as the agent: the first 16 hex characters of the SHA-256 of the key."""
    return hashlib.sha256(k.encode("utf-8")).hexdigest()[:16]


def km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


def box(lat, lon, half_km):
    dlat = half_km / 111.0
    dlon = half_km / (111.0 * max(0.2, math.cos(math.radians(lat))))
    return (lon - dlon, lat - dlat, lon + dlon, lat + dlat)


def province_of(addr):
    """Two letter province from an Overture address, or '' when it is not in Canada or not known."""
    country = str(addr.get("country") or "").upper()
    if country and country not in ("CA", "CAN", "CANADA"):
        return None
    region = unicodedata.normalize("NFKD", str(addr.get("region") or "")).encode("ascii", "ignore").decode().upper().replace("CA-", "").strip()
    if region in PROVINCES:
        return region
    if region in PROVINCE_NAMES:
        return PROVINCE_NAMES[region]
    pc = str(addr.get("postcode") or "").strip().upper()
    if re.match(r"^[A-Z]\d[A-Z]", pc):
        if pc[0] == "X":
            return "NU" if pc[:3] in ("X0A", "X0B", "X0C") else "NT"
        return POSTAL_PROV.get(pc[0], "")
    if region and region not in PROVINCES:
        return None   # a region that is not a Canadian province (a US state across the border)
    return ""


def instagram_of(urls):
    for s in urls:
        m = IG_RE.search(str(s or ""))
        if m and not IG_RESERVED.match(m.group(1)):
            return m.group(1).rstrip(".").lower()
    return ""


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data, indent=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=indent, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)


def now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ============================== settings ==============================
def load_config():
    cfg = json.loads(json.dumps(DEFAULTS))
    given = load_json(CONFIG_FILE, {})
    for k, v in (given or {}).items():
        if k not in DEFAULTS or v is None:
            continue
        if isinstance(DEFAULTS[k], dict) and isinstance(v, dict):
            cfg[k].update(dict((a, b) for a, b in v.items() if a in DEFAULTS[k] or k == "seed"))
        elif isinstance(DEFAULTS[k], bool):
            cfg[k] = bool(v)
        elif isinstance(DEFAULTS[k], (int, float)) and isinstance(v, (int, float)):
            cfg[k] = type(DEFAULTS[k])(v)
        elif isinstance(DEFAULTS[k], list) and isinstance(v, list):
            cfg[k] = v
    if os.environ.get("BATCH_SIZE", "").strip():
        try:
            cfg["batch_size"] = int(os.environ["BATCH_SIZE"])
        except ValueError:
            pass
    prov = [p.strip().upper() for p in (os.environ.get("PROVINCES") or "").split(",") if p.strip()]
    if prov:
        cfg["include_provinces"] = prov
    cfg["batch_size"] = max(10, min(2000, int(cfg["batch_size"])))
    total = sum(max(0.0, float(x)) for x in cfg["mix"].values()) or 1.0
    cfg["mix"] = dict((k, max(0.0, float(cfg["mix"].get(k, 0))) / total) for k in ("seed", "home", "national"))
    cfg["include_provinces"] = [p for p in (x.upper() for x in cfg["include_provinces"]) if p in PROVINCES]
    return cfg


def province_allowed(prov, cfg):
    if not prov:
        return True
    if prov == "QC" and not cfg["include_quebec"]:
        return False
    return not cfg["include_provinces"] or prov in cfg["include_provinces"]


# ============================== the category model ==============================
class Model:
    def __init__(self, rules, blocked):
        self.default = rules["default"]
        self.rules = [(r, re.compile(r["re"], re.I)) for r in rules["rules"]]
        self.blocked = [norm(b) for b in blocked or [] if b]

    def classify(self, tokens, name="", extra=()):
        """The first rule that matches the most specific category word. Restricted industries are checked on every word and the name."""
        toks = [norm(t) for t in tokens if t]
        everything = toks + [norm(t) for t in extra if t] + [norm(name)]
        for r, rx in self.rules:
            if r["kind"] == "restricted" and any(rx.search(t) for t in everything):
                return r
        if any(b and any(re.search(r"\b" + re.escape(b), t) for t in everything) for b in self.blocked):
            return {"id": "blocked", "kind": "restricted", "label": "Blocked in the agent's settings", "fit": 0, "terminal": "countertop", "family": "other", "why": "on the agent's blocked list"}
        for t in toks:
            for r, rx in self.rules:
                if r["kind"] != "restricted" and rx.search(t):
                    return r
        return self.default

    def business_from_name(self, name):
        """For a listing with no category: the first business rule its name matches (never an exclusion: 'Bank Street Brakes' is a garage)."""
        n = norm(name)
        for r, rx in self.rules:
            if r["kind"] == "business" and r["id"] != self.default["id"] and rx.search(n):
                return r
        return None


def category_of(row):
    """The business type: Overture's most specific category, its parent path and the basic category."""
    tax = row.get("taxonomy") or {}
    cats = row.get("categories") or {}
    basic = row.get("basic_category") or ""
    primary = (tax.get("primary") if isinstance(tax, dict) else "") or basic or (cats.get("primary") if isinstance(cats, dict) else "") or ""
    path = list((tax.get("hierarchy") if isinstance(tax, dict) else None) or [])
    if not path:
        path = [x for x in [basic, primary] if x]
    if primary and (not path or path[-1] != primary):
        path.append(primary)
    alts = list((tax.get("alternate") if isinstance(tax, dict) else None) or (cats.get("alternate") if isinstance(cats, dict) else None) or [])
    path = [str(x) for x in path if x]
    tokens = [primary] + list(reversed(path[:-1])) + ([basic] if basic and basic != primary else [])
    return str(primary or "unknown"), path, [str(x) for x in tokens if x], [str(x) for x in alts if x]


def market_key(m, prov=""):
    """How the agent names a market in its results: province and place name, like ON:ottawa."""
    return "%s:%s" % ((m.get("prov") or prov or "").upper(), norm(m.get("name") or ""))


def label_of(key):
    w = str(key or "").replace("_", " ").strip()
    return w[:1].upper() + w[1:]


# ============================== learning from the agent's results ==============================
class Learned:
    """Multipliers from the agent's results. Only used where there is enough data, and always within limits."""

    def __init__(self, feedback):
        fb = feedback or {}
        self.types = fb.get("by_type") or {}
        self.markets = fb.get("by_market") or {}
        imp = sum(int(v.get("imported", 0)) for v in self.types.values())
        qual = sum(int(v.get("qualified", 0)) for v in self.types.values())
        cont = sum(int(v.get("contacted", 0)) for v in self.types.values())
        rep = sum(int(v.get("replied", 0)) for v in self.types.values())
        self.g_q = (qual + 5.0) / (imp + 10.0)          # share of businesses where the agent found an email
        self.g_r = (rep + 1.0) / (cont + 50.0)          # reply share
        self.updated = fb.get("updated_at", "")

    def _mult(self, v):
        imp, qual = int(v.get("imported", 0)), int(v.get("qualified", 0))
        cont, rep = int(v.get("contacted", 0)), int(v.get("replied", 0))
        if imp < 10:
            return 1.0, imp, ""
        q = (qual + 10 * self.g_q) / (imp + 10.0)
        m = max(0.5, min(1.6, q / self.g_q))
        why = "%d%% of %d had an email" % (round(100.0 * qual / imp), imp)
        if cont >= 20:
            r = (rep + 30 * self.g_r) / (cont + 30.0)
            m *= max(0.6, min(1.6, r / self.g_r))
            why += ", %d replies from %d emailed" % (rep, cont)
        return max(0.4, min(2.0, m)), imp, why

    def type_mult(self, key):
        return self._mult(self.types.get(key) or {})

    def market_mult(self, mid):
        m, n, why = self._mult(self.markets.get(mid) or {})
        return max(0.7, min(1.4, m)), n, why


# ============================== where Canada's markets are ==============================
def radius_for(pop):
    if not pop:
        return 3.0
    return max(2.0, min(25.0, 2.0 * (max(pop, 300) / 500.0) ** 0.33))


def build_markets(core, release, cfg, log):
    """Every city, town and village in Canada from Overture's divisions (localities). Rebuilt once per release."""
    markets = {}
    try:
        reader = read_type(core, "division", CANADA_BBOX, release, log)
    except Exception as e:
        log("  (could not read Overture's list of places: %s)" % e)
        reader = None
    if reader is not None:
        import pyarrow.compute as pc
        for batch in reader:
            names = batch.schema.names
            batch = batch.select([c for c in DIVISION_COLS if c in names])
            if "country" in batch.schema.names and "subtype" in batch.schema.names:
                mask = pc.and_(pc.equal(batch.column("country"), "CA"), pc.equal(batch.column("subtype"), "locality"))
                batch = batch.filter(mask)
            for d in batch.to_pylist():
                if str(d.get("country") or "") != "CA" or str(d.get("subtype") or "") != "locality":
                    continue
                prov = str(d.get("region") or "").upper().replace("CA-", "")
                b = d.get("bbox") or {}
                if prov not in PROVINCES or not b:
                    continue
                nm = (d.get("names") or {}).get("primary") or ""
                lat, lon = (b["ymin"] + b["ymax"]) / 2.0, (b["xmin"] + b["xmax"]) / 2.0
                pop = d.get("population")
                mid = "m" + hashlib.sha1(str(d.get("id")).encode()).hexdigest()[:10]
                markets[mid] = {"name": nm, "prov": prov, "lat": round(lat, 5), "lon": round(lon, 5), "pop": int(pop) if pop else None}
    source = "divisions"
    if not markets:
        log("  (the list of Canadian places could not be read; searching a grid over Canada instead)")
        markets, source = grid_markets(), "grid"
    # The same place listed twice (same name, same province, under 3 km apart): keep the one with a population
    by_name = {}
    for mid, m in sorted(markets.items(), key=lambda kv: -(kv[1]["pop"] or 0)):
        k = (norm(m["name"]), m["prov"])
        dupe = next((o for o in by_name.get(k, []) if km(o["lat"], o["lon"], m["lat"], m["lon"]) < 3), None)
        if dupe:
            m["dupe_of"] = dupe["id"]
        else:
            m["id"] = mid
            by_name.setdefault(k, []).append(m)
    markets = dict((mid, m) for mid, m in markets.items() if not m.get("dupe_of"))
    for mid, m in markets.items():
        m.pop("id", None)
        m["r"] = round(radius_for(m["pop"]), 1)
    return {"release": release, "built_at": now_iso(), "source": source, "markets": markets, "areas": {}}


def grid_markets():
    """Fallback when the divisions data cannot be read: half degree squares over southern Canada and the main northern towns."""
    out = {}
    lat = 42.0
    while lat < 62.0:
        lon = -141.0
        while lon < -52.5:
            mid = "g%.1f_%.1f" % (lat, lon)
            out[mid] = {"name": "Grid %.1f, %.1f" % (lat + 0.25, lon + 0.25), "prov": "", "lat": lat + 0.25, "lon": lon + 0.25, "pop": None, "grid": True}
            lon += 0.5
        lat += 0.5
    return out


def seed_market(cfg):
    s = cfg["seed"]
    return {"name": s.get("name", "Seed"), "prov": s.get("province", cfg["home_province"]), "lat": float(s["lat"]), "lon": float(s["lon"]),
            "pop": None, "r": float(s.get("radius_km", 25)), "seed": True}


def areas_of(mid, m, cfg):
    """The squares a market is searched in: one for a town, a grid (centre first) for a city."""
    a_km = float(cfg["area_km"])
    if m.get("grid"):
        return [(mid + "#0", (m["lon"] - 0.25, m["lat"] - 0.25, m["lon"] + 0.25, m["lat"] + 0.25), 0.0)]
    r = float(m.get("r") or 3)
    if 2 * r <= a_km * 1.5:
        return [(mid + "#0", box(m["lat"], m["lon"], r), 0.0)]
    n = int(math.ceil(2 * r / a_km))
    out = []
    for i in range(n):
        for j in range(n):
            dy = (i + 0.5) * a_km - r
            dx = (j + 0.5) * a_km - r
            dist = math.hypot(dx, dy)
            if dist > r + a_km * 0.5:
                continue
            lat = m["lat"] + dy / 111.0
            lon = m["lon"] + dx / (111.0 * max(0.2, math.cos(math.radians(m["lat"]))))
            out.append(("%s#%d.%d" % (mid, i, j), box(lat, lon, a_km / 2.0), dist))
    out.sort(key=lambda x: x[2])
    return out


# ============================== reading Overture ==============================
def read_type(core, kind, bbox, release, log):
    try:
        reader = core.record_batch_reader(kind, bbox=bbox, release=release, stac=True)
    except Exception as e:   # the file index is a shortcut; without it the search is slower but still works
        log("  (file index unavailable: %s; searching without it)" % e)
        reader = core.record_batch_reader(kind, bbox=bbox, release=release, stac=False)
    return reader


def places_in(core, bbox, release, log):
    reader = read_type(core, "place", bbox, release, log)
    if reader is None:
        return
    for batch in reader:
        batch = batch.select([c for c in WANTED if c in batch.schema.names])
        for row in batch.to_pylist():
            b = row.get("bbox")
            if b:
                row["_lat"], row["_lon"] = (b["ymin"] + b["ymax"]) / 2, (b["xmin"] + b["xmax"]) / 2
            yield row


def cache_file_index(core):
    """The overturemaps reader downloads the same file index for every search; keep one copy per run."""
    orig = getattr(core, "urlopen", None)
    if orig is None or getattr(orig, "_cached", False):
        return
    store = {}

    def cached(url, *a, **k):
        if isinstance(url, str) and url.endswith("collections.parquet"):
            if url not in store:
                with orig(url, *a, **k) as r:
                    store[url] = r.read()
            return io.BytesIO(store[url])
        return orig(url, *a, **k)
    cached._cached = True
    core.urlopen = cached


def latest_release(core):
    """The newest Overture release. Normally from Overture's release list; if that list cannot be read,
    from the release folders in Overture's public storage (the same place the places come from)."""
    try:
        return core.get_latest_release()
    except Exception as e:
        print("  (release list unavailable: %s; reading the release folders instead)" % e, flush=True)
        from pyarrow import fs
        s3 = fs.S3FileSystem(anonymous=True, region="us-west-2")
        infos = s3.get_file_info(fs.FileSelector("overturemaps-us-west-2/release/"))
        names = sorted(i.base_name for i in infos if re.match(r"^\d{4}-\d{2}-\d{2}\.\d+$", i.base_name))
        if not names:
            raise
        return names[-1]


# ============================== one business ==============================
def evaluate(row, model, learned, cfg, market_id, market, known, sent, types):
    """One Overture place: a candidate with a score and reasons, or the reason it is left out."""
    pid = row.get("id")
    if not pid:
        return None, "no id"
    names = row.get("names") or {}
    name = ((names.get("primary") if isinstance(names, dict) else str(names)) or "").strip()
    key, path, tokens, alts = category_of(row)
    guess = None
    if key == "unknown" and name:   # no category in the listing: the name often says what the business is
        guess = model.business_from_name(name)
        if guess:
            key, tokens = "no_category_" + guess["id"], [name]
    t = types.setdefault(key, {"label": ("No category, the name suggests: " + guess["label"]) if guess else label_of(key), "path": path,
                               "first_seen": now_iso(), "seen": 0, "viable": 0, "picked": 0, "excluded": 0, "prov": {}})
    t["seen"] += 1
    if path and len(path) > len(t.get("path") or []):
        t["path"] = path
    if not name:
        return None, "no name"
    addrs = row.get("addresses") or []
    addr = (addrs[0] if addrs else None) or {}
    prov = province_of(addr) if addr else ""
    if prov is None:
        return None, "outside Canada"
    phone = phone_fmt(first(row.get("phones")))
    if not prov:
        if not addr and not (phone and phone_digits(phone)[:3] in CA_AREA_CODES):
            return None, "no address"
        prov = market.get("prov") or ""
    pv = t["prov"].setdefault(prov or "?", {"seen": 0, "viable": 0, "picked": 0})
    pv["seen"] += 1
    rule = model.classify(tokens, name, alts)
    if guess and rule["kind"] != "restricted":
        rule = guess   # the name's business match, not an exclusion its words happen to contain
    if rule["kind"] == "restricted":   # one business (often by its name); the type keeps the rule its category gives
        t["excluded"] += 1
        t["restricted"] = t.get("restricted", 0) + 1
        return None, "restricted industry"
    t["rule"], t["fit"], t["terminal"], t["family"], t["kind"] = rule["id"], rule["fit"], rule["terminal"], rule["family"], rule["kind"]
    if rule["kind"] == "exclude" or rule["fit"] <= 0:
        t["excluded"] += 1
        return None, "not a merchant prospect"
    if rule["fit"] < cfg["min_fit"]:
        t["excluded"] += 1
        return None, "rarely takes card payments in person"
    if str(row.get("operating_status") or "open").lower() != "open":
        return None, "closed"
    conf = row.get("confidence")
    conf = float(conf) if conf is not None else 0.5
    if conf < cfg["min_confidence"]:
        return None, "uncertain listing"
    brand = row.get("brand") or {}
    bname = (brand.get("names") or {}).get("primary") if isinstance(brand.get("names"), dict) else brand.get("names")
    sites = [x for x in (row.get("websites") or []) if x]
    website = next((x for x in sites if not SOCIAL_RE.search(x)), "")
    if (isinstance(brand, dict) and (brand.get("wikidata") or bname)) or CHAIN_RE.search(name) or (website and CHAIN_RE.search(website)):
        return None, "chain or franchise"
    if pid in sent:
        return None, "duplicate"
    email = first(row.get("emails"))
    ig = instagram_of(list(row.get("socials") or []) + sites)
    if not website and not phone and not email and not (ig and rule["family"] == "restaurant"):
        return None, "no contact details"
    city = (addr.get("locality") or market.get("name") or "").strip()
    dom = domain_of(website)
    keys = [k for k in ["d:" + dom if dom else "", "p:" + phone_digits(phone) if phone else "", "n:" + name.lower()[:120] + "|" + city.lower()] if k]
    if any(key_hash(k) in known for k in keys):
        return None, "duplicate"

    terminal = rule["terminal"]
    if MOBILE_RE.search(" ".join(norm(x) for x in tokens + [name])):
        terminal = "mobile"
    reasons, score = [], 0.0
    base = rule["fit"] * 0.55
    score += base
    reasons.append("%s: %s (%d) [estimate]" % (rule["label"], rule["why"], rule["fit"]))
    if guess:
        reasons.append("the listing has no category; the type is guessed from the business name [estimate]")
    freeform = str(addr.get("freeform") or "")
    if freeform and STREET_RE.search(freeform):
        score += 12
        reasons.append("has a street address, so it serves customers at a location [listing]")
    elif terminal == "mobile":
        score += 6
        reasons.append("works at the customer's site, a mobile terminal fits [estimate]")
    else:
        reasons.append("no street address listed [listing]")
    if dom:
        score += 8
    if phone:
        score += 8
    if email:
        score += 5
    contact = [x for x, ok in (("website", dom), ("phone", phone), ("email", email), ("Instagram", ig and not dom)) if ok]
    reasons.append("listed %s [listing]" % ", ".join(contact))
    cpts = max(0.0, min(10.0, (conf - 0.5) * 20))
    score += cpts
    if cpts >= 5:
        reasons.append("Overture is %d%% sure the listing is right [listing]" % round(conf * 100))
    tm, tn, twhy = learned.type_mult(key)
    if tn >= 10 and abs(tm - 1) >= 0.05:
        adj = max(-10.0, min(10.0, 10 * (tm - 1) / 0.6))
        score += adj
        reasons.append("%s this type: %s [learned]" % ("up for" if adj > 0 else "down for", twhy))
    mm, mn, mwhy = learned.market_mult(market_key(market, prov))
    if mn >= 10 and abs(mm - 1) >= 0.05:
        adj = max(-5.0, min(5.0, 5 * (mm - 1) / 0.4))
        score += adj
        reasons.append("%s this area: %s [learned]" % ("up for" if adj > 0 else "down for", mwhy))
    score = int(round(max(0.0, min(100.0, score))))
    if score < cfg["min_score"]:
        return None, "low score"
    t["viable"] += 1
    pv["viable"] += 1
    a_line = ", ".join(x for x in [freeform, addr.get("locality"), prov, addr.get("postcode")] if x)
    cat_words = label_of(key)
    return {
        "Company": name[:120], "Category": cat_words, "Website": website, "Email": email, "Phone": phone, "Address": a_line,
        "City": city, "Province": prov, "Instagram": ig, "Sector": rule["family"], "BusinessType": key, "Score": score,
        "ScoreReasons": "; ".join(reasons)[:900], "Terminal": terminal, "Market": market.get("name", ""), "OvertureId": pid,
        "_type": key, "_rule": rule["id"], "_fit": rule["fit"], "_tm": tm, "_mid": market_id, "_prov": prov, "_keys": keys, "_dom": dom, "_phone": phone_digits(phone),
    }, ""


# ============================== planning which areas to search ==============================
class Planner:
    def __init__(self, index, cfg, learned, totals):
        self.cfg, self.learned = cfg, learned
        self.areas = index.setdefault("areas", {})
        self.release = index.get("release")
        self.groups = {"seed": [], "home": [], "national": {}}
        self.prov_weight = {}
        seed = seed_market(cfg)
        self.seed = seed
        mk = dict(index["markets"])
        mk["seed"] = seed
        self.markets = mk
        self.covered = covered_markets(mk, cfg)
        for mid, m in mk.items():
            if mid in self.covered:
                continue
            prov = m.get("prov") or ""
            if prov and not province_allowed(prov, cfg):
                continue
            if mid == "seed":
                g = "seed"
            elif prov == cfg["home_province"]:
                g = "home"
            else:
                g = "national"
            if g == "national":
                self.prov_weight[prov] = self.prov_weight.get(prov, 0) + (m["pop"] or 500)
            for aid, b, dist in areas_of(mid, m, cfg):
                if m.get("grid"):
                    dist = km(m["lat"], m["lon"], seed["lat"], seed["lon"])
                pri = self.priority(aid, mid, m)
                if pri <= 0:
                    continue
                item = {"aid": aid, "mid": mid, "bbox": b, "pri": pri, "pop": m["pop"] or 0, "prov": prov, "dist": dist,
                        "small": g != "seed" and (m["pop"] or 0) < cfg["small_town_pop"]}
                if g == "national":
                    self.groups["national"].setdefault(prov, []).append(item)
                else:
                    self.groups[g].append(item)
        key = lambda x: (-x["pri"], x["dist"], -x["pop"], x["aid"])
        self.groups["seed"].sort(key=key)
        self.groups["home"].sort(key=key)
        for p in self.groups["national"]:
            self.groups["national"][p].sort(key=key)
        self.prov_weight = dict((p, math.sqrt(w)) for p, w in self.prov_weight.items())
        self.picked_prov = dict(totals.get("prov_picked") or {})
        self.gathered = {}
        self.prov_searched = {}   # areas already searched in this release, per province
        for aid, a in self.areas.items():
            p = (mk.get(a.get("mid") or aid.split("#")[0]) or {}).get("prov")
            if p and a.get("rel") == self.release:
                self.prov_searched[p] = self.prov_searched.get(p, 0) + 1
        self.small_done = {"seed": [0, 0], "home": [0, 0], "national": [0, 0]}

    def priority(self, aid, mid, m):
        a = self.areas.get(aid)
        mult = self.learned.market_mult(market_key(m))[0]
        if not a:
            return 1.0 * mult
        if a.get("rel") != self.release:
            return 0.7 * mult   # a new Overture release: new businesses may be listed since the last search
        if a.get("status") in ("empty", "exhausted"):
            return 0.0
        left = int(a.get("left", 0))
        return (0.4 + 0.5 * min(1.0, left / 40.0)) * mult if left > 0 else 0.0

    def has(self, g):
        if g == "national":
            return any(self.groups["national"].values())
        return bool(self.groups[g])

    def next(self, g, want_small=None):
        if g != "national":
            return self._take(self.groups[g], g, want_small)
        live = [p for p, q in self.groups["national"].items() if q]
        if not live:
            return None
        first = [p for p in live if not self.prov_searched.get(p)]
        if first:   # a province or territory not searched yet in this release goes first, the bigger one first
            prov = max(first, key=lambda p: (self.prov_weight.get(p, 1), p))
            self.prov_searched[prov] = 1
            return self._take(self.groups["national"][prov], g, want_small)
        # the province furthest behind its share (by population, square root so small provinces still get turns)
        done = lambda p: self.picked_prov.get(p, 0) + self.gathered.get(p, 0) / self.cfg["pool_factor"]
        tot_w = sum(self.prov_weight.get(p, 1) for p in live) or 1.0
        tot_p = sum(done(p) for p in live) + 1.0
        prov = max(live, key=lambda p: ((self.prov_weight.get(p, 1) / tot_w) * tot_p - done(p), self.prov_weight.get(p, 1)))
        self.prov_searched[prov] = self.prov_searched.get(prov, 0) + 1
        return self._take(self.groups["national"][prov], g, want_small)

    def _take(self, q, g, want_small=None):
        if not q:
            return None
        sd = self.small_done[g]
        if want_small is None:
            want_small = (sd[1] + 1) * self.cfg["small_town_share"] > sd[0] + 0.5
        pick = next((x for x in q if x["small"] == want_small), q[0])
        q.remove(pick)
        sd[1] += 1
        if pick["small"]:
            sd[0] += 1
        return pick

    def note_gathered(self, prov, n):
        if prov:
            self.gathered[prov] = self.gathered.get(prov, 0) + n

    def note_picked(self, prov, n):
        self.picked_prov[prov] = self.picked_prov.get(prov, 0) + n


def covered_markets(mk, cfg):
    """Towns inside a bigger market's search circle (or the seed's) are searched as part of it, not twice."""
    big = [(mid, m) for mid, m in mk.items() if not m.get("grid") and (mid == "seed" or (m.get("r") or 0) * 2 > cfg["area_km"] * 1.5)]
    big.sort(key=lambda kv: (0 if kv[0] == "seed" else 1, -(kv[1].get("pop") or 0)))
    covered = {}
    for mid, m in mk.items():
        if mid == "seed" or m.get("grid"):
            continue
        for bid, b in big:
            if bid == mid or abs(m["lat"] - b["lat"]) > 0.3 or (bid != "seed" and (b.get("pop") or 0) <= (m.get("pop") or 0)):
                continue
            if km(m["lat"], m["lon"], b["lat"], b["lon"]) <= b["r"] * (1.0 if bid == "seed" else 0.8):
                covered[mid] = bid
                break
    return covered


# ============================== choosing the batch ==============================
def explore_priority(c, types, learned, cfg):
    """Why a not yet tested type deserves a look: related to a type that does well, or a strong type new to a province."""
    t = types.get(c["_type"]) or {}
    pri, why = c["_fit"] / 100.0, ""
    path = t.get("path") or []
    parent = path[-2] if len(path) >= 2 else ""
    if parent:
        for k, o in types.items():
            if k != c["_type"] and len(o.get("path") or []) >= 2 and o["path"][-2] == parent:
                m, n, _ = learned.type_mult(k)
                if n >= 15 and m >= 1.15:
                    pri += 1.0
                    why = "related to %s, which does well" % o.get("label", k)
                    break
    m, n, _ = learned.type_mult(c["_type"])
    if not why and n >= 15 and m >= 1.15 and int(((t.get("prov") or {}).get(c["_prov"]) or {}).get("picked", 0)) == 0:
        pri += 0.5
        why = "does well elsewhere, not tried in %s yet" % c["_prov"]
    return pri, why


def pick_batch(cands, n, cfg, types, learned, area_used, type_used, area_cap=None):
    """Best first, without starving anyone: a share for untested types, then weighted turns across the model's kinds
    of business, and within each kind, turns across its Overture types. Turns go by kind first because Overture splits
    some kinds much finer than others (dozens of restaurant types, one clothing store type)."""
    chosen = []
    by_type = {}
    for c in sorted(cands, key=lambda c: (-c["Score"], c["OvertureId"])):
        by_type.setdefault(c["_type"], []).append(c)
    rule_of = dict((t, q[0].get("_rule") or t) for t, q in by_type.items())
    by_rule = {}
    for t in sorted(by_type, key=lambda t: (-by_type[t][0]["Score"], t)):   # within a kind, its best type first
        by_rule.setdefault(rule_of[t], []).append(t)
    type_cap = max(3, int(math.ceil(cfg["batch_size"] * cfg["per_type_cap_share"])))
    area_cap = area_cap or cfg["per_area_cap"]
    caps = {}
    for t in by_type:
        caps[t] = type_cap
        if rule_of[t] == "unknown" and learned.type_mult(t)[1] < 10:
            caps[t] = 3   # a type the model does not know: a few at a time until the agent's results say more

    def take(t):
        q = by_type.get(t) or []
        while q:
            c = q.pop(0)
            if area_used.get(c["_aid"], 0) >= area_cap:
                continue
            chosen.append(c)
            area_used[c["_aid"]] = area_used.get(c["_aid"], 0) + 1
            type_used[t] = type_used.get(t, 0) + 1
            return True
        return False

    # 1. Exploration: types handed over fewer than explore_min_picks times so far, one kind of business at a time
    budget = int(round(n * cfg["explore_share"]))
    untested = {}
    for t, q in by_type.items():
        hist = int((types.get(t) or {}).get("picked", 0)) + type_used.get(t, 0)
        if q and hist < cfg["explore_min_picks"]:
            pri, why = explore_priority(q[0], types, learned, cfg)
            untested.setdefault(rule_of[t], []).append([pri, t, why, 0])
    for r in untested:
        untested[r].sort(key=lambda x: (-x[0], x[1]))
    order = sorted(untested, key=lambda r: (-untested[r][0][0], r))
    while budget > 0 and order and len(chosen) < n:
        for r in list(order):
            if budget <= 0 or len(chosen) >= n:
                break
            q = untested[r]
            u = q.pop(0)
            if type_used.get(u[1], 0) < caps[u[1]] and take(u[1]):
                c = chosen[-1]
                c["ScoreReasons"] = ("tried as a new type: " + (u[2] or "not tested yet") + " [explore]; " + c["ScoreReasons"])[:900]
                c["_explore"] = True
                budget -= 1
                u[3] += 1
                if u[3] < 3 and by_type.get(u[1]):
                    q.append(u)   # at most 3 exploration picks per type in a batch
            if not q:
                order.remove(r)

    # 2. Weighted turns across the kinds of business (fit times what was learned, with a floor),
    #    and within a kind, its types in turn
    weights = {}
    for r, ts in by_rule.items():
        live_ts = [t for t in ts if by_type.get(t)]
        if live_ts:
            fit = by_type[live_ts[0]][0]["_fit"] / 100.0
            tm = sum((by_type[t][0]["_tm"] or 1.0) for t in live_ts) / len(live_ts)
            weights[r] = fit * tm
    top = max(weights.values()) if weights else 1.0
    for r in weights:
        weights[r] = max(weights[r], top * cfg["min_type_weight"])
    current = dict((r, 0.0) for r in weights)
    turn = dict((r, 0) for r in weights)
    open_types = lambda r: [t for t in by_rule[r] if by_type.get(t) and type_used.get(t, 0) < caps[t]]
    while len(chosen) < n:
        live = [r for r in weights if open_types(r)]
        if not live:
            break
        total = sum(weights[r] for r in live)
        for r in live:
            current[r] += weights[r]
        best = max(live, key=lambda r: (current[r], r))
        current[best] -= total
        ts = open_types(best)
        t = ts[turn[best] % len(ts)]
        turn[best] += 1
        if not take(t):
            by_type[t] = []
    # still short (few types in these areas): fill from the best that is left, still one area at a time
    if len(chosen) < n:
        rest = sorted((c for t, q in by_type.items() if caps.get(t, type_cap) > 3 for c in q), key=lambda c: (-c["Score"], c["OvertureId"]))
        for c in rest:
            if len(chosen) >= n:
                break
            if area_used.get(c["_aid"], 0) >= area_cap:
                continue
            chosen.append(c)
            area_used[c["_aid"]] = area_used.get(c["_aid"], 0) + 1
            type_used[c["_type"]] = type_used.get(c["_type"], 0) + 1
    return chosen


# ============================== the run ==============================
def main(core=None, clock=time.time, log=None):
    t0 = clock()
    log = log or (lambda s: print(s, flush=True))
    cfg = load_config()
    os.makedirs(FEED_DIR, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    rules = load_json(RULES_FILE, None)
    if not rules:
        raise SystemExit("model/category_rules.json is missing")
    model = Model(rules, cfg["blocked"])
    if core is None:
        from overturemaps import core
        try:
            cache_file_index(core)
        except Exception as e:
            log("  (could not keep the file index between searches: %s)" % e)
    sent = set()
    if os.path.exists(SENT_FILE):
        with open(SENT_FILE) as f:
            sent = set(x.strip() for x in f if x.strip())
    known = set()
    for path in (KNOWN_FILE, SENT_KEYS_FILE):   # the same business under a new Overture id is still the same business
        if os.path.exists(path):
            with open(path) as f:
                known |= set(x.strip()[:16] for x in f if x.strip() and not x.startswith("#"))
    learned = Learned(load_json(FEEDBACK_FILE, {}))
    release = latest_release(core)
    log("Overture release %s; %d businesses handed over before, %d known to the agent" % (release, len(sent), len(known)))

    index = load_json(MARKETS_FILE, {})
    if index.get("release") != release or not index.get("markets"):
        old_areas = index.get("areas") or {}
        index = build_markets(core, release, cfg, log)
        index["areas"] = old_areas   # what was searched before is kept: a new release only lowers its priority
        log("Markets: %d places in Canada (%s)" % (len(index["markets"]), index["source"]))
    types_state = load_json(TYPES_FILE, {"types": {}, "totals": {}})
    types = types_state.setdefault("types", {})
    totals = types_state.setdefault("totals", {})
    for t in types.values():
        t["_seen_before"] = True

    planner = Planner(index, cfg, learned, totals)
    n = cfg["batch_size"]
    targets = dict((g, int(round(n * cfg["mix"][g]))) for g in ("seed", "home", "national"))
    pools = {"seed": [], "home": [], "national": []}
    seen_ids, seen_dom, seen_phone = set(), set(), set()
    excluded, area_log = {}, []
    scanned = 0
    deadline = t0 + cfg["max_minutes"] * 60
    areas_done = 0
    budget = dict((g, max(3, int(round(cfg["max_areas"] * cfg["mix"][g])))) for g in ("seed", "home", "national"))
    spent = {"seed": 0, "home": 0, "national": 0}
    # Enough areas, not only enough candidates: one city square can hold thousands of businesses, but no area gives
    # more than per_area_cap a batch, and the rest of Canada should reach every province it can in one run.
    area_need = dict((g, int(math.ceil(targets[g] * cfg["pool_factor"] / float(max(1, cfg["per_area_cap"]))))) for g in ("seed", "home", "national"))
    if targets["national"] > 0:
        area_need["national"] = max(area_need["national"], min(budget["national"], len([p for p, q in planner.groups["national"].items() if q])))
    for g in area_need:
        area_need[g] = min(area_need[g], budget[g]) if targets[g] > 0 else 0
    areas_with = {"seed": 0, "home": 0, "national": 0}
    small_need = dict((g, int(math.ceil(area_need[g] * cfg["small_town_share"])) if g != "seed" else 0) for g in area_need)
    small_with = {"seed": 0, "home": 0, "national": 0}
    while areas_done < cfg["max_areas"] and clock() < deadline:
        # the group furthest from having enough candidates goes next, within its share of the areas;
        # a group with nothing left (or no share) gives its share away
        open_groups = [g for g in ("seed", "home", "national") if planner.has(g) and (targets[g] > 0 or not any(targets[x] > 0 and planner.has(x) for x in ("seed", "home", "national")))]
        if not open_groups:
            break
        need = dict((g, targets[g] * cfg["pool_factor"] - len(pools[g])) for g in open_groups)
        missing = sum(max(0, targets[g] * cfg["pool_factor"] - len(pools[g])) for g in ("seed", "home", "national") if g not in open_groups)
        short = lambda x: max(need[x] / max(1.0, targets[x] * cfg["pool_factor"]), (area_need[x] - areas_with[x]) / float(max(1, area_need[x])))
        hungry = [g for g in open_groups if short(g) > 0]
        if not hungry and missing <= 0:
            break
        within = [g for g in hungry if spent[g] < budget[g]] or hungry or open_groups
        g = max(within, key=lambda x: (short(x), -("seed", "home", "national").index(x)))
        want_small = None
        if g != "seed":
            small_have = sum(1 for c in pools[g] if c["_small"])
            want_small = small_have < targets[g] * cfg["small_town_share"] * cfg["pool_factor"] or small_with[g] < small_need[g]
        area = planner.next(g, want_small)
        if not area:
            continue
        spent[g] += 1
        areas_done += 1
        m = planner.markets[area["mid"]]
        a_seen = a_viable = 0
        a_by = {}
        for row in places_in(core, area["bbox"], release, log):
            scanned += 1
            a_seen += 1
            if row.get("id") in seen_ids:
                continue
            seen_ids.add(row.get("id"))
            try:
                c, why = evaluate(row, model, learned, cfg, area["mid"], m, known, sent, types)
            except Exception as e:   # one odd listing never stops the run
                c, why = None, "unreadable listing"
                if excluded.get(why, 0) < 3:
                    log("  (skipped a listing the engine could not read: %s)" % e)
            if not c:
                excluded[why] = excluded.get(why, 0) + 1
                continue
            if not province_allowed(c["_prov"], cfg):
                excluded["province not included (Quebec waits for French outreach)" if c["_prov"] == "QC" else "province not included"] = excluded.get("province not included (Quebec waits for French outreach)" if c["_prov"] == "QC" else "province not included", 0) + 1
                continue
            if (c["_dom"] and c["_dom"] in seen_dom) or (c["_phone"] and c["_phone"] in seen_phone):
                excluded["duplicate"] = excluded.get("duplicate", 0) + 1
                continue
            if c["_dom"]:
                seen_dom.add(c["_dom"])
            if c["_phone"]:
                seen_phone.add(c["_phone"])
            c["_aid"], c["_group"], c["_small"] = area["aid"], g, area["small"]
            a_viable += 1
            a_by[c["_type"]] = a_by.get(c["_type"], 0) + 1
            pools[g].append(c)
        planner.note_gathered(m.get("prov") or area["prov"], a_viable)
        if a_viable:
            areas_with[g] += 1
            if area["small"]:
                small_with[g] += 1
        if m.get("grid") and not m.get("prov"):
            provs = [c["_prov"] for c in pools[g] if c["_aid"] == area["aid"]]
            if provs:
                m["prov"] = max(set(provs), key=provs.count)
        planner.areas[area["aid"]] = {"rel": release, "q": now_iso(), "seen": a_seen, "viable": a_viable, "picked": 0, "left": a_viable,
                                      "status": "empty" if a_viable == 0 else "open", "mid": area["mid"]}
        area_log.append({"area": area["aid"], "market": m.get("name"), "prov": m.get("prov") or area["prov"], "group": g, "seen": a_seen, "viable": a_viable})
        log("  %-28s %-3s %-8s %5d places, %4d candidates" % ((m.get("name") or "?")[:28], m.get("prov") or "", g, a_seen, a_viable))

    # choose the batch: each group's share first, then whatever is left from any group
    area_used, type_used, out = {}, {}, []
    group_cap = {}
    for g in ("seed", "home", "national"):
        want = min(targets[g], n - len(out))
        # spread a group's picks over the areas it searched: no area gives much more than its fair part
        n_areas = len(set(c["_aid"] for c in pools[g])) or 1
        cap = group_cap[g] = min(cfg["per_area_cap"], max(5, int(math.ceil(want * 1.5 / n_areas))))
        got = []
        if g != "seed":   # small towns first get their share, so big cities do not crowd them out
            got = pick_batch([c for c in pools[g] if c["_small"]], int(round(want * cfg["small_town_share"])), cfg, types, learned, area_used, type_used, cap)
        ids = set(c["OvertureId"] for c in got)
        got += pick_batch([c for c in pools[g] if c["OvertureId"] not in ids], want - len(got), cfg, types, learned, area_used, type_used, cap)
        out.extend(got)
        ids = set(c["OvertureId"] for c in got)
        pools[g] = [c for c in pools[g] if c["OvertureId"] not in ids]
    if len(out) < n:
        rest = pools["seed"] + pools["home"] + pools["national"]
        out.extend(pick_batch(rest, n - len(out), cfg, types, learned, area_used, type_used))
    out.sort(key=lambda c: (-c["Score"], c["OvertureId"]))

    # remember what was handed over and what is left in each area
    stamp = dt.datetime.fromtimestamp(clock(), dt.timezone.utc).strftime("%Y-%m-%d-%H%M")
    for c in out:
        t = types[c["_type"]]
        t["picked"] += 1
        t["last_picked"] = stamp
        pv = t["prov"].setdefault(c["_prov"] or "?", {"seen": 0, "viable": 0, "picked": 0})
        pv["picked"] += 1
        a = planner.areas.get(c["_aid"])
        if a:
            a["picked"] += 1
            a["left"] = max(0, a["left"] - 1)
        planner.note_picked(c["_prov"], 1)
    for aid, a in planner.areas.items():
        if a.get("rel") == release and a.get("status") == "open" and a.get("left", 0) <= 0:
            a["status"] = "exhausted"
    totals["prov_picked"] = planner.picked_prov
    totals["picked"] = int(totals.get("picked", 0)) + len(out)
    totals["scanned"] = int(totals.get("scanned", 0)) + scanned
    totals["runs"] = int(totals.get("runs", 0)) + 1
    for k, v in excluded.items():
        totals.setdefault("excluded", {})[k] = int(totals.get("excluded", {}).get(k, 0)) + v
    new_types = sorted(k for k, t in types.items() if not t.get("_seen_before"))
    for t in types.values():
        t.pop("_seen_before", None)

    if out:
        for path in (os.path.join(FEED_DIR, "latest.csv"), os.path.join(FEED_DIR, "history", stamp + ".csv")):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
                w.writeheader()
                w.writerows(out)
        with open(SENT_FILE, "a") as f:
            for c in out:
                f.write(c["OvertureId"] + "\n")
        with open(SENT_KEYS_FILE, "a") as f:
            for c in out:
                for k in c["_keys"]:
                    if not k.startswith("n:"):
                        f.write(key_hash(k) + "\n")
    save_json(MARKETS_FILE, index)
    save_json(TYPES_FILE, types_state)
    report = make_report(stamp, release, cfg, out, pools, excluded, area_log, scanned, clock() - t0, index, types, totals, new_types, planner)
    report["areas_needed"], report["areas_with_candidates"], report["area_cap_by_group"] = area_need, areas_with, group_cap
    report["small_town_areas"] = small_with
    save_json(os.path.join(FEED_DIR, "report", "latest.json"), report, indent=1)
    coverage = make_coverage(index, types, totals, cfg, release, planner)
    save_json(os.path.join(FEED_DIR, "coverage.json"), coverage, indent=1)
    with open(os.path.join(FEED_DIR, "COVERAGE.md"), "w") as f:
        f.write(coverage_md(coverage, report))
    with open(RUNS_FILE, "a") as f:
        f.write(json.dumps({"stamp": stamp, "release": release, "picked": len(out), "scanned": scanned, "areas": len(area_log),
                            "excluded": excluded, "seconds": int(clock() - t0)}, sort_keys=True) + "\n")
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)
    if not out:
        log("No new businesses this run. See feed/report/latest.json for why.")
        return report
    log("Wrote %d new businesses to feed/history/%s.csv (%s)" % (len(out), stamp, ", ".join("%s %d" % kv for kv in sorted(report["picked_by"]["province"].items()))))
    return report


def count_by(rows, key):
    out = {}
    for r in rows:
        k = r.get(key) or "?"
        out[k] = out.get(k, 0) + 1
    return out


def make_report(stamp, release, cfg, out, pools, excluded, area_log, scanned, seconds, index, types, totals, new_types, planner):
    left = sum(len(p) for p in pools.values())
    return {
        "stamp": stamp, "release": release, "seconds": int(seconds), "batch_size": cfg["batch_size"],
        "settings": dict((k, cfg[k]) for k in ("mix", "home_province", "include_provinces", "include_quebec", "min_score", "explore_share", "small_town_share", "max_areas", "max_minutes")),
        "areas_searched": len(area_log), "areas": area_log, "places_scanned": scanned,
        "candidates": len(out) + left, "picked": len(out), "explored": sum(1 for c in out if c.get("_explore")),
        "left_for_later": left, "excluded": excluded, "duplicates_removed": int(excluded.get("duplicate", 0)),
        "picked_by": {"province": count_by(out, "Province"), "sector": count_by(out, "Sector"), "type": count_by(out, "BusinessType"), "kind": count_by(out, "_rule"),
                      "group": count_by([{"g": c["_group"]} for c in out], "g"), "terminal": count_by(out, "Terminal"),
                      "small_town": sum(1 for c in out if c.get("_small"))},
        "score": {"top": out[0]["Score"] if out else None, "median": sorted(c["Score"] for c in out)[len(out) // 2] if out else None,
                  "lowest": out[-1]["Score"] if out else None},
        "new_business_types": [{"key": k, "label": types[k]["label"], "rule": types[k].get("rule", ""), "fit": types[k].get("fit"), "viable": types[k].get("viable", 0)}
                               for k in new_types if types[k].get("kind") == "business" and types[k].get("viable")][:200],
        "new_types_left_out": sum(1 for k in new_types if not (types[k].get("kind") == "business" and types[k].get("viable"))),
        "markets_known": len(index["markets"]), "markets_source": index.get("source"),
    }


def make_coverage(index, types, totals, cfg, release, planner):
    provs = dict((p, {"name": PROVINCES[p], "markets_known": 0, "people_in_known_markets": 0, "markets_searched": 0, "areas_searched": 0,
                      "businesses_seen": 0, "viable_seen": 0, "picked": 0, "status": ""}) for p in PROVINCES)
    searched_markets = {}
    for aid, a in (index.get("areas") or {}).items():
        mid = a.get("mid") or aid.split("#")[0]
        m = planner.markets.get(mid) or {}
        p = m.get("prov")
        if p in provs:
            provs[p]["areas_searched"] += 1
            searched_markets.setdefault(p, set()).add(mid)
    for mid, m in index["markets"].items():
        p = m.get("prov")
        if p in provs:
            provs[p]["markets_known"] += 1
            provs[p]["people_in_known_markets"] += int(m.get("pop") or 0)
    for t in types.values():
        for p, v in (t.get("prov") or {}).items():
            if p in provs:
                provs[p]["businesses_seen"] += int(v.get("seen", 0))
                provs[p]["viable_seen"] += int(v.get("viable", 0))
                provs[p]["picked"] += int(v.get("picked", 0))
    gaps = []
    for p, v in provs.items():
        v["markets_searched"] = len(searched_markets.get(p, ()))
        if p == "QC" and not cfg["include_quebec"]:
            v["status"] = "waiting: outreach in French is not set up, so Quebec is counted but not searched"
        elif cfg["include_provinces"] and p not in cfg["include_provinces"]:
            v["status"] = "left out by the settings"
        elif not v["markets_searched"]:
            v["status"] = "not searched yet"
        elif v["markets_searched"] < max(1, v["markets_known"]) * 0.1:
            v["status"] = "started"
        else:
            v["status"] = "under way"
        if v["status"] in ("not searched yet", "started") and v["markets_known"]:
            gaps.append("%s: %d of %d places searched so far" % (PROVINCES[p], v["markets_searched"], v["markets_known"]))
    tlist = []
    for k, t in types.items():
        tlist.append({"key": k, "label": t.get("label", k), "rule": t.get("rule", ""), "fit": t.get("fit"), "family": t.get("family", ""),
                      "terminal": t.get("terminal", ""), "kind": t.get("kind", ""), "seen": t.get("seen", 0), "viable": t.get("viable", 0),
                      "picked": t.get("picked", 0), "provinces": sorted(p for p, v in (t.get("prov") or {}).items() if v.get("seen")),
                      "first_seen": t.get("first_seen", "")})
    tlist.sort(key=lambda x: (-x["picked"], -x["viable"], x["key"]))
    for t in tlist:
        if t["kind"] == "business" and t["viable"] >= 20 and t["picked"] == 0:
            gaps.append("%s: %d good businesses seen, none handed over yet" % (t["label"], t["viable"]))
    return {"updated_at": now_iso(), "release": release, "markets_known": len(index["markets"]), "markets_source": index.get("source"),
            "totals": {"runs": totals.get("runs", 0), "places_scanned": totals.get("scanned", 0), "picked": totals.get("picked", 0),
                       "excluded": totals.get("excluded", {}), "business_types_seen": len(types),
                       "business_types_viable": sum(1 for t in types.values() if t.get("viable")),
                       "business_types_picked": sum(1 for t in types.values() if t.get("picked"))},
            "provinces": provs, "business_types": tlist[:400], "gaps": gaps[:60]}


def coverage_md(cov, rep):
    lines = ["# Lead feed coverage", "", "Updated %s (Overture release %s). The outreach agent shows the same numbers in its Coverage tab." % (cov["updated_at"], cov["release"]), "",
             "## Last run", "", "%d businesses handed over from %d areas (%d places read). %d left for later runs." % (rep["picked"], rep["areas_searched"], rep["places_scanned"], rep["left_for_later"]), "",
             "## Provinces and territories", "", "| | Places known | Places searched | Businesses seen | Good fits | Handed over | Status |", "|---|---:|---:|---:|---:|---:|---|"]
    for p, v in cov["provinces"].items():
        lines.append("| %s | %d | %d | %d | %d | %d | %s |" % (v["name"], v["markets_known"], v["markets_searched"], v["businesses_seen"], v["viable_seen"], v["picked"], v["status"]))
    lines += ["", "## Business types (top 60)", "", "| Type | Model rule | Fit | Seen | Good fits | Handed over |", "|---|---|---:|---:|---:|---:|"]
    for t in cov["business_types"][:60]:
        lines.append("| %s | %s | %s | %d | %d | %d |" % (t["label"], t["rule"], t["fit"], t["seen"], t["viable"], t["picked"]))
    if cov["gaps"]:
        lines += ["", "## Gaps", ""] + ["- " + g for g in cov["gaps"]]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
