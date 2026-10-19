"""CPOS lead feed: once a week, pick new Ontario businesses from Overture Maps for the outreach agent.

Overture Maps is open map data (places are licensed CDLA Permissive 2.0, fine for business use).
This script reads the latest release straight from Overture's public storage, keeps independent
Ontario businesses in the agent's industries that have a phone or website, skips any it sent
before, and writes a new file in feed/history/ (plus a copy as feed/latest.csv). The agent imports each new file by itself.

Settings (environment variables, all optional):
  BATCH_SIZE  how many new businesses per run (default 300)
  CITIES      comma separated city names to limit the search (default: Ottawa first, then all of Ontario)
"""
import csv
import datetime as dt
import math
import os
import re
import sys

BATCH_SIZE = int(os.environ.get("BATCH_SIZE") or 300)
HERE = os.path.dirname(os.path.abspath(__file__))
SENT_FILE = os.path.join(HERE, "state", "sent_ids.txt")
FEED_DIR = os.path.join(HERE, "feed")

# Same order as the agent: the Ottawa area first, then the rest of Ontario. [name, lat, lon, radius km]
CITIES = [
    ["Ottawa", 45.4215, -75.6972, 25], ["Kingston", 44.2312, -76.4860, 10], ["Cornwall", 45.0213, -74.7303, 7], ["Brockville", 44.5895, -75.6843, 5],
    ["Pembroke", 45.8263, -77.1106, 5], ["Belleville", 44.1628, -77.3832, 7], ["Peterborough", 44.3091, -78.3197, 8],
    ["Toronto", 43.6532, -79.3832, 20], ["Mississauga", 43.5890, -79.6441, 12], ["Brampton", 43.7315, -79.7624, 12], ["Hamilton", 43.2557, -79.8711, 15],
    ["London", 42.9849, -81.2453, 12], ["Markham", 43.8561, -79.3370, 10], ["Vaughan", 43.8372, -79.5083, 10], ["Kitchener", 43.4516, -80.4925, 9],
    ["Windsor", 42.3149, -83.0364, 10], ["Richmond Hill", 43.8828, -79.4403, 8], ["Oakville", 43.4675, -79.6877, 9], ["Burlington", 43.3255, -79.7990, 9],
    ["Sudbury", 46.4917, -80.9930, 12], ["Oshawa", 43.8971, -78.8658, 9], ["Barrie", 44.3894, -79.6903, 9], ["St. Catharines", 43.1594, -79.2469, 9],
    ["Cambridge", 43.3616, -80.3144, 8], ["Guelph", 43.5448, -80.2482, 8], ["Whitby", 43.8975, -78.9429, 7], ["Ajax", 43.8509, -79.0204, 7],
    ["Thunder Bay", 48.3809, -89.2477, 12], ["Waterloo", 43.4643, -80.5204, 6], ["Brantford", 43.1394, -80.2644, 8], ["Niagara Falls", 43.0896, -79.0849, 8],
    ["Sarnia", 42.9745, -82.4066, 8], ["Sault Ste. Marie", 46.5219, -84.3461, 8], ["North Bay", 46.3091, -79.4608, 7], ["Welland", 42.9922, -79.2483, 6],
    ["Newmarket", 44.0592, -79.4613, 6], ["Timmins", 48.4758, -81.3305, 8], ["Woodstock", 43.1315, -80.7467, 6], ["Orillia", 44.6082, -79.4197, 6],
    ["Stratford", 43.3700, -80.9822, 5], ["Chatham", 42.4048, -82.1910, 7], ["Owen Sound", 44.5690, -80.9406, 5], ["Collingwood", 44.5008, -80.2169, 5],
]

# Industries in the order the agent prefers them (lowest churn first). Matched against Overture's category words.
SECTORS = [
    ("transport", r"taxi|limo|limousine|towing|tow truck|courier|moving|movers|trucking|freight|shuttle|chauffeur"),
    ("automotive", r"\bauto|automotive|car repair|car wash|car dealer|used car|tire|tyre|mechanic|collision|body shop|oil change|transmission|muffler|windshield|motorcycle|detailing"),
    ("dental", r"dentist|dental|orthodont|endodont|periodont|denturist"),
    ("vet", r"veterinar|animal hospital|animal clinic|pet groom|dog groom|pet sitting|pet boarding|kennel"),
    ("salon", r"hair|barber|beauty salon|nail|\bsalon|tattoo|eyelash|lash|brow|waxing"),
    ("landscaper", r"landscap|lawn|garden center|garden centre|nursery|tree service|snow removal|irrigation"),
    ("trades", r"plumb|electrician|electrical|hvac|heating|air condition|roofing|roofer|carpent|contractor|handyman|painting|painter|locksmith|cleaning|janitor|flooring|renovat|masonry|pest control|appliance repair"),
    ("wellness", r"gym|fitness|yoga|pilates|physio|physical therap|chiropract|massage therap|optometr|optician|naturopath|acupunct|martial art|crossfit|dance studio|boxing"),
    ("restaurant", r"restaurant|cafe|coffee|bakery|\bbar\b|pub|brewery|pizza|sushi|diner|bistro|grill|eatery|catering|ice cream|dessert|juice|bubble tea|tea room|deli|food truck|burger|shawarma"),
    ("retail", r"florist|jewel|furniture|hardware|book store|bookstore|gift|clothing|apparel|shoe|sporting|bicycle|pet store|pet supply|electronics|mobile phone|cell phone|convenience|grocery|boutique|toy|music store|liquor"),
    ("professional", r"accountant|accounting|bookkeep|tax prep|tax service|lawyer|law firm|legal service|notary|insurance|real estate|financial advis|architect"),
    ("spa", r"\bspa\b|day spa|med spa|massage"),
]
SECTOR_RES = [(name, re.compile(rx, re.I)) for name, rx in SECTORS]
NOT_PROSPECTS = re.compile(r"atm|\bbank|credit union|school|college|university|church|mosque|synagogue|temple|government|police|fire station|hospital|park\b|playground|parking|gas station|hotel|motel|airport|station|post office|library|museum|community center|community centre|cemetery|shopping center|shopping centre|mall\b", re.I)
CHAIN_RE = re.compile(r"\b(speedy|midas|mr\.? ?lube|jiffy lube|canadian tire|kal tire|ok tire|active green|napa auto ?pro|firestone|goodyear|fountain tire|great canadian oil change|costco|walmart|tim hortons|mcdonald'?s|subway|starbucks|a&w|pizza pizza|domino'?s|little caesars|pizza hut|kfc|wendy'?s|burger king|popeyes|dairy queen|shoppers drug mart|rexall|dollarama|great clips|first choice haircutters|magicuts|pet valu|petsmart|home depot|rona|staples|best buy|the brick|leon'?s|sleep country|lenscrafters|specsavers|h&r block|u-?haul|enterprise rent|budget rent|jugo juice|booster juice|freshii|mary brown'?s|harvey'?s|swiss chalet|boston pizza|east side mario'?s|the keg|montana'?s|kelsey'?s|milestones|st\.? louis bar)\b", re.I)
IG_RE = re.compile(r"instagram\.com/(?:#!/)?@?([A-Za-z0-9._]{1,30})", re.I)
IG_RESERVED = re.compile(r"^(p|reel|reels|explore|stories|accounts|tv|direct|about|legal|developer|developers|web|share|instagram|login)$", re.I)
# A social page or a booking site is not the business's own website (same list as the agent)
SOCIAL_RE = re.compile(r"(google\.|goo\.gl|g\.page|facebook\.|fb\.com|instagram\.|yelp\.|tripadvisor\.|linkedin\.|twitter\.|x\.com|tiktok\.|youtube\.|ubereats\.|doordash\.|skipthedishes\.|opentable\.|linktr\.ee|yellowpages\.|canpages\.|411\.ca|booksy\.|fresha\.|vagaro\.|square\.site|wa\.me)", re.I)
ONTARIO_POSTCODE = re.compile(r"^\s*[KLMNP]\d[A-Z]", re.I)
# Only the columns the script uses: much less to download
WANTED = ["id", "names", "categories", "basic_category", "taxonomy", "confidence", "websites", "socials", "emails", "phones", "brand", "addresses", "operating_status", "bbox"]

COLUMNS = ["Company", "Category", "Website", "Email", "Phone", "Address", "City", "Instagram", "Sector", "OvertureId"]


def first(v):
    """First non-empty item of a list (or the value itself)."""
    if isinstance(v, (list, tuple)):
        for x in v:
            if x:
                return x.get("value") or x.get("url") if isinstance(x, dict) else x
        return ""
    return v or ""


def category_of(row):
    """Overture has renamed its category fields over time; take whichever this release has."""
    for key in ("basic_category",):
        if row.get(key):
            return str(row[key])
    tax = row.get("taxonomy") or {}
    if isinstance(tax, dict) and tax.get("primary"):
        return str(tax["primary"])
    cats = row.get("categories") or {}
    if isinstance(cats, dict) and cats.get("primary"):
        return str(cats["primary"])
    return ""


def sector_of(category):
    words = category.replace("_", " ")
    if not words or NOT_PROSPECTS.search(words):
        return None
    for name, rx in SECTOR_RES:
        if rx.search(words):
            return name
    return None


def phone_fmt(p):
    d = re.sub(r"\D", "", str(p or ""))
    if len(d) == 11 and d[0] == "1":
        d = d[1:]
    return "(%s) %s-%s" % (d[:3], d[3:6], d[6:]) if len(d) == 10 else ""


def km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def in_ontario(addr):
    """Ontario only: Gatineau and the US are next door."""
    if str(addr.get("country") or "CA").upper() not in ("CA", "CAN", "CANADA"):
        return False
    region = str(addr.get("region") or "").upper().replace("CA-", "").strip()
    if region:
        return region in ("ON", "ONT", "ONTARIO")
    return bool(ONTARIO_POSTCODE.match(str(addr.get("postcode") or "")))


def instagram_of(urls):
    for s in urls:
        m = IG_RE.search(str(s or ""))
        if m and not IG_RESERVED.match(m.group(1)):
            return m.group(1).rstrip(".").lower()
    return ""


def lead_from(row, city, sent):
    """One Overture place to one lead row, or None when it is not a fit."""
    pid = row.get("id")
    if not pid or pid in sent:
        return None
    names = row.get("names") or {}
    name = (names.get("primary") if isinstance(names, dict) else str(names)) or ""
    name = name.strip()
    if not name or CHAIN_RE.search(name):
        return None
    brand = row.get("brand") or {}
    if isinstance(brand, dict) and (brand.get("wikidata") or ((brand.get("names") or {}).get("primary") if isinstance(brand.get("names"), dict) else brand.get("names"))):
        return None
    status = str(row.get("operating_status") or "open").lower()
    if status != "open":
        return None
    conf = row.get("confidence")
    if conf is not None and float(conf) < 0.5:
        return None
    addrs = row.get("addresses") or []
    addr = (addrs[0] if addrs else None) or {}
    if not addrs or not in_ontario(addr):
        return None
    category = category_of(row)
    sector = sector_of(category)
    if not sector:
        return None
    sites = [x for x in (row.get("websites") or []) if x]
    website = next((x for x in sites if not SOCIAL_RE.search(x)), "")
    phone = phone_fmt(first(row.get("phones")))
    ig = instagram_of(list(row.get("socials") or []) + sites)
    if not website and not phone and not (ig and sector == "restaurant"):
        return None   # a restaurant with only Instagram still works: Jed messages it there
    if website and CHAIN_RE.search(website):
        return None
    lat, lon = row.get("_lat"), row.get("_lon")
    if lat is not None and km(city[1], city[2], lat, lon) > city[3]:
        return None
    cat_words = category.replace("_", " ").strip()
    return {
        "Company": name[:120], "Category": cat_words[:1].upper() + cat_words[1:], "Website": website, "Email": first(row.get("emails")),
        "Phone": phone, "Address": ", ".join(x for x in [addr.get("freeform"), addr.get("locality"), "ON", addr.get("postcode")] if x),
        "City": (addr.get("locality") or city[0]).strip(), "Instagram": ig, "Sector": sector, "OvertureId": pid,
        "_rank": [n for n, _ in SECTORS].index(sector), "_conf": float(conf or 0.5),
    }


def pick(candidates, room):
    """A mix of every industry, so the agent can keep learning which ones answer. Each round takes one business
    per industry, lowest churn industries first; within an industry the most certain listings (with a website) go first."""
    groups = {}
    for r in sorted(candidates, key=lambda r: (-r["_conf"], 0 if r["Website"] else 1, r["Company"].lower())):
        groups.setdefault(r["_rank"], []).append(r)
    order = sorted(groups)
    out = []
    while len(out) < room and any(groups[k] for k in order):
        for k in order:
            if groups[k] and len(out) < room:
                out.append(groups[k].pop(0))
    return out


def bbox_for(city):
    lat, lon, r = city[1], city[2], city[3]
    dlat = r / 111.0
    dlon = r / (111.0 * math.cos(math.radians(lat)))
    return (lon - dlon, lat - dlat, lon + dlon, lat + dlat)


def places_near(city, release):
    """Every Overture place in the city's box."""
    from overturemaps import core   # imported here so the logic above can be tested without network access
    try:
        reader = core.record_batch_reader("place", bbox=bbox_for(city), release=release, stac=True)
    except Exception as e:   # the file index is a shortcut; without it the search is slower but still works
        print("  (file index unavailable: %s; searching without it)" % e, flush=True)
        reader = core.record_batch_reader("place", bbox=bbox_for(city), release=release, stac=False)
    if reader is None:
        return
    for batch in reader:
        batch = batch.select([c for c in WANTED if c in batch.schema.names])
        for row in batch.to_pylist():
            b = row.get("bbox")
            if b:
                row["_lat"], row["_lon"] = (b["ymin"] + b["ymax"]) / 2, (b["xmin"] + b["xmax"]) / 2
            yield row


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


def main():
    os.makedirs(FEED_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(SENT_FILE), exist_ok=True)
    sent = set()
    if os.path.exists(SENT_FILE):
        with open(SENT_FILE) as f:
            sent = set(x.strip() for x in f if x.strip())
    only = [c.strip().lower() for c in (os.environ.get("CITIES") or "").split(",") if c.strip()]
    cities = [c for c in CITIES if not only or c[0].lower() in only]
    from overturemaps import core
    release = latest_release(core)
    print("Overture release %s, %d businesses already sent before" % (release, len(sent)), flush=True)
    out, by = [], {}
    for city in cities:
        room = BATCH_SIZE - len(out)
        if room <= 0:
            break
        found = []
        for row in places_near(city, release):
            lead = lead_from(row, city, sent)
            if lead:
                found.append(lead)
                sent.add(lead["OvertureId"])   # never twice, even across cities whose boxes overlap
        chosen = pick(found, room)
        keep = set(r["OvertureId"] for r in chosen)
        for r in found:
            if r["OvertureId"] not in keep:
                sent.discard(r["OvertureId"])   # not in this batch: still available next time
        out.extend(chosen)
        if chosen:
            by[city[0]] = len(chosen)
        print("%s: %d candidates, %d picked" % (city[0], len(found), len(chosen)), flush=True)
    if not out:
        print("No new businesses found. Every listed city may already be covered.", file=sys.stderr)
        return
    # One file per run (date and time, so two runs on one day never overwrite each other). The agent imports each file once.
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d-%H%M")
    for path in (os.path.join(FEED_DIR, "latest.csv"), os.path.join(FEED_DIR, "history", stamp + ".csv")):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
            w.writeheader()
            w.writerows(out)
    with open(SENT_FILE, "a") as f:
        for r in out:
            f.write(r["OvertureId"] + "\n")
    print("Wrote %d new businesses to feed/history/%s.csv (%s)" % (len(out), stamp, ", ".join("%s %d" % kv for kv in by.items())))


if __name__ == "__main__":
    main()
