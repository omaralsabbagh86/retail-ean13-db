#!/usr/bin/env python3
"""
Retail EAN-13 database - multi-source daily updater.

Public database files (4 columns):  data/ean_<key>.csv
    ean13, description, created_at, modified_at     (UTC ISO timestamps)
Helper files (which source owns each record): meta/src_<key>.csv
    ean13, source

Sources (enable/disable and tune in config.json):
    item_master     your own item master CSV in inbox/item_master.csv   (highest trust)
    lookup_apis     barcode APIs for barcodes listed in inbox/missing_barcodes.txt
    openfacts       Open Food/Beauty/Products/Pet Food Facts (full seed once, then daily deltas)
    wikidata        products with a GTIN on Wikidata
    usda            USDA FoodData Central branded foods
    discogs         music releases (CD/vinyl barcodes)
    openlibrary     books (ISBN-13 = EAN-13)
    webdatacommons  product data embedded in web shops (Common Crawl extraction)

A higher-priority source may overwrite a description from a lower (or equal) one.
A lower-priority source only fills barcodes nobody else has.

Usage:
    python update_db.py                       run every enabled source that is due
    python update_db.py --sources usda,wikidata   force-run only these sources
"""
import csv
import gzip
import hashlib
import html
import io
import json
import os
import re
import sys
import time
import traceback
import zipfile
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, OrderedDict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
META_DIR = ROOT / "meta"
INBOX = ROOT / "inbox"
STATE_FILE = ROOT / "state.json"
CONFIG_FILE = ROOT / "config.json"
HEADER = ["ean13", "description", "created_at", "modified_at"]

csv.field_size_limit(sys.maxsize)
START = time.time()

DEFAULT_CONFIG = {
    "user_agent": "RetailEAN13DB/2.0 (github.com/YOUR_USERNAME/YOUR_REPO)",
    "max_runtime_minutes": 300,
    "max_description_length": 250,
    "skip_prefixes": ["2", "02", "04", "05", "98", "99"],
    "sources": {
        "manual":         {"enabled": False, "priority": 1000},   # edits/imports from the admin web app
        "item_master":    {"enabled": True,  "priority": 100, "every_days": 0},
        "lookup_apis":    {"enabled": True,  "priority": 60,  "every_days": 0,
                           "max_attempts": 7,
                           "budgets": {"upcitemdb": 95, "ean_search": 0, "go_upc": 0, "barcodelookup": 0}},
        "openfacts":      {"enabled": True,  "priority": 70,  "every_days": 0},
        "wikidata":       {"enabled": True,  "priority": 50,  "every_days": 7},
        "usda":           {"enabled": True,  "priority": 80,  "every_days": 30, "zip_url": ""},
        "discogs":        {"enabled": False, "priority": 40,  "every_days": 30, "first_wins": True},
        "openlibrary":    {"enabled": False, "priority": 40,  "every_days": 30, "first_wins": True},
        "webdatacommons": {"enabled": False, "priority": 20,  "every_days": 0,  "first_wins": True,
                           "update_existing": False, "file_list_url": "", "file_list_urls": [],
                           "parts_per_run": 50, "wanted_only": True},
    },
}


def load_config():
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if CONFIG_FILE.exists():
        user = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        for k, v in user.items():
            if k == "sources":
                for name, sv in v.items():
                    cfg["sources"].setdefault(name, {}).update(sv)
            else:
                cfg[k] = v
    return cfg


CFG = load_config()


def log(*a):
    print(f"[{int(time.time() - START):>5}s]", *a, flush=True)


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def minutes_used():
    return (time.time() - START) / 60


# ------------------------------------------------------------------ helpers

def make_session():
    s = requests.Session()
    s.headers["User-Agent"] = CFG["user_agent"]
    retry = Retry(total=4, backoff_factor=3, status_forcelist=[500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


HTTP = make_session()


def clean(text):
    if text is None:
        return ""
    if isinstance(text, (list, tuple)):
        text = ",".join(str(t) for t in text if t)
    return re.sub(r"\s+", " ", html.unescape(str(text))).strip()


def fix_case(s):
    return s.title() if s and s.isupper() else s


def join_desc(brand, name, extra=""):
    name, brand, extra = clean(name), clean(clean(brand).split(",")[0]), clean(extra)
    if not name:
        return ""
    parts = []
    if brand and brand.lower() not in name.lower():
        parts.append(brand)
    parts.append(name)
    if extra and extra.lower() not in name.lower():
        parts.append(extra)
    return " ".join(parts)


def ean_check_digit(first12):
    return (10 - sum(int(c) * (3 if i % 2 else 1) for i, c in enumerate(first12)) % 10) % 10


def to_ean13(code, pad_short=False):
    """Normalise to a valid EAN-13 or return None.
    pad_short=True completes any shorter number with leading zeros (used for your own item master)."""
    code = re.sub(r"\D", "", str(code or ""))
    if len(code) == 14 and code.startswith("0"):
        code = code[1:]
    if pad_short and 0 < len(code) < 13:
        code = code.zfill(13)
    if len(code) == 8:
        code = code.zfill(13)          # EAN-8 stored with leading zeros (checksum stays valid)
    if len(code) == 12:
        code = "0" + code
    if len(code) != 13 or int(code) == 0:
        return None
    return code if ean_check_digit(code[:12]) == int(code[12]) else None


def isbn10_to_ean13(isbn):
    isbn = re.sub(r"[^0-9Xx]", "", str(isbn or ""))
    if len(isbn) != 10:
        return None
    body = "978" + isbn[:9]
    return body + str(ean_check_digit(body))


def stream_gz_lines(url):
    r = HTTP.get(url, stream=True, timeout=(30, 600))
    r.raise_for_status()
    return io.TextIOWrapper(gzip.GzipFile(fileobj=r.raw), encoding="utf-8", errors="replace")


def download_to_temp(url, suffix):
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as fh, HTTP.get(url, stream=True, timeout=(30, 600)) as r:
        r.raise_for_status()
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
    return path


def atomic_write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, path)


# ------------------------------------------------------------------ database

class Database:
    def __init__(self):
        self.rows = {}          # ean -> [description, created_at, modified_at, source]
        self.dirty = set()
        self.migrate = False
        self.stats = {}
        self.current = None     # source currently running
        self.seen = None        # per-run first-wins set
        self.ts = now()
        self.skip = tuple(CFG["skip_prefixes"])
        self.maxlen = CFG["max_description_length"]
        self.pad_short = False
        self.deleted = set()    # barcodes removed in the admin app - never re-added automatically

    @staticmethod
    def key(ean):
        # books (978/979) get finer shards because there are millions of them
        return ean[:5] if ean.startswith(("978", "979")) else ean[:3]

    @staticmethod
    def priority(source):
        return CFG["sources"].get(source, {}).get("priority", 0)

    def load(self):
        deleted_file = META_DIR / "deleted.csv"
        if deleted_file.exists():
            with deleted_file.open(newline="", encoding="utf-8") as fh:
                self.deleted = {row["ean13"] for row in csv.DictReader(fh)}
        sources = {}
        for f in META_DIR.glob("src_*.csv"):
            with f.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    sources[row["ean13"]] = row["source"]
        for f in DATA_DIR.glob("ean_*.csv"):
            file_key = f.stem[4:]
            with f.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    ean = row["ean13"]
                    if self.key(ean) != file_key:
                        self.migrate = True
                    self.rows[ean] = [row["description"], row["created_at"], row["modified_at"],
                                      sys.intern(sources.get(ean, "openfacts"))]
        log(f"Loaded {len(self.rows):,} records" + (" (re-sharding needed)" if self.migrate else ""))

    def begin(self, source):
        self.current = source
        self.stats[source] = {"added": 0, "updated": 0}
        self.seen = set() if CFG["sources"][source].get("first_wins") else None
        self.pad_short = source in ("item_master", "manual")

    def upsert(self, code, desc):
        source = self.current
        ean = to_ean13(code, self.pad_short)
        if not ean or ean.startswith(self.skip) or ean in self.deleted:
            return False
        desc = clean(desc)[: self.maxlen]
        if len(desc) < 3 or desc.isdigit():
            return False
        if self.seen is not None:
            if ean in self.seen:
                return False
            self.seen.add(ean)

        cur = self.rows.get(ean)
        if cur is None:
            self.rows[ean] = [desc, self.ts, self.ts, source]
            self.stats[source]["added"] += 1
        else:
            new_p, old_p = self.priority(source), self.priority(cur[3])
            allow_equal = CFG["sources"][source].get("update_existing", True)
            if new_p < old_p or (new_p == old_p and not allow_equal):
                return False
            changed = False
            if cur[0] != desc:
                cur[0], cur[2] = desc, self.ts
                self.stats[source]["updated"] += 1
                changed = True
            if cur[3] != source:
                cur[3] = source
                changed = True
            if not changed:
                return False
        self.dirty.add(self.key(ean))
        return True

    def save(self):
        if self.migrate:
            self.dirty = {self.key(e) for e in self.rows}
        if not self.dirty:
            return
        grouped = {k: [] for k in self.dirty}
        for ean, r in self.rows.items():
            k = self.key(ean)
            if k in grouped:
                grouped[k].append((ean, r[0], r[1], r[2], r[3]))
        for k, rows in grouped.items():
            rows.sort()
            atomic_write_csv(DATA_DIR / f"ean_{k}.csv", HEADER, (r[:4] for r in rows))
            atomic_write_csv(META_DIR / f"src_{k}.csv", ["ean13", "source"], ((r[0], r[4]) for r in rows))
        if self.migrate:
            valid = {self.key(e) for e in self.rows}
            for folder, prefix in ((DATA_DIR, "ean_"), (META_DIR, "src_")):
                for f in folder.glob(prefix + "*.csv"):
                    if f.stem[len(prefix):] not in valid:
                        f.unlink()
            self.migrate = False
        log(f"Saved {len(self.dirty)} shard(s)")
        self.dirty = set()


# ------------------------------------------------------------------ sources

def src_item_master(db, st, cfg):
    files = sorted(INBOX.glob("item_master*.csv"))
    if not files:
        log("No inbox/item_master*.csv - skipping")
        return True
    for f in files:
        digest = hashlib.sha256(f.read_bytes()).hexdigest()
        if st.get("hashes", {}).get(f.name) == digest:
            log(f"{f.name} unchanged")
            continue
        text = f.read_text(encoding="utf-8-sig", errors="replace")
        dialect = csv.Sniffer().sniff(text[:5000], delimiters=",;\t|")
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        cols = reader.fieldnames or []
        bc = next((c for c in cols if re.search(r"ean|barcode|gtin|upc", c, re.I)), None)
        dc = next((c for c in cols if re.search(r"desc|name|title", c, re.I)), None)
        if not bc or not dc:
            log(f"{f.name}: need a barcode column and a description column, found {cols}")
            continue
        for row in reader:
            db.upsert(row.get(bc), row.get(dc))
        st.setdefault("hashes", {})[f.name] = digest
    return True


# --- lookup APIs for barcodes you actually scan

def api_upcitemdb(ean):
    r = HTTP.get("https://api.upcitemdb.com/prod/trial/lookup", params={"upc": ean}, timeout=30)
    if r.status_code == 429:
        raise PermissionError("rate limit")
    items = r.json().get("items") or []
    time.sleep(11)  # trial plan allows only a few calls per minute
    return join_desc(items[0].get("brand"), items[0].get("title")) if items else None


def api_ean_search(ean):
    token = os.environ.get("EAN_SEARCH_TOKEN")
    if not token:
        raise PermissionError("no token")
    r = HTTP.get("https://api.ean-search.org/api", timeout=30,
                 params={"token": token, "op": "barcode-lookup", "ean": ean, "format": "json"})
    data = r.json()
    if isinstance(data, list) and data and data[0].get("name"):
        return data[0]["name"]
    if isinstance(data, list) and data and "error" in data[0] and "limit" in str(data[0]["error"]).lower():
        raise PermissionError("limit")
    return None


def api_go_upc(ean):
    key = os.environ.get("GO_UPC_KEY")
    if not key:
        raise PermissionError("no key")
    r = HTTP.get(f"https://go-upc.com/api/v1/code/{ean}", timeout=30,
                 headers={"Authorization": f"Bearer {key}"})
    if r.status_code in (401, 403, 429):
        raise PermissionError(r.status_code)
    p = (r.json() or {}).get("product") or {}
    return join_desc(p.get("brand"), p.get("name")) if p.get("name") else None


def api_barcodelookup(ean):
    key = os.environ.get("BARCODELOOKUP_KEY")
    if not key:
        raise PermissionError("no key")
    r = HTTP.get("https://api.barcodelookup.com/v3/products", timeout=30,
                 params={"barcode": ean, "key": key})
    if r.status_code in (401, 403, 429):
        raise PermissionError(r.status_code)
    if r.status_code == 404:
        return None
    prods = (r.json() or {}).get("products") or []
    return join_desc(prods[0].get("brand"), prods[0].get("title")) if prods else None


LOOKUP_PROVIDERS = [("upcitemdb", api_upcitemdb), ("ean_search", api_ean_search),
                    ("go_upc", api_go_upc), ("barcodelookup", api_barcodelookup)]


def src_lookup_apis(db, st, cfg):
    inbox_files = sorted(INBOX.glob("missing*.txt"))
    wanted = []
    for f in inbox_files:
        for token in re.findall(r"\d{12,14}", f.read_text(encoding="utf-8", errors="replace")):
            ean = to_ean13(token)
            if ean and ean not in wanted:
                wanted.append(ean)
    attempts = st.setdefault("attempts", {})
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    budgets = dict(cfg.get("budgets", {}))
    not_found = []

    for ean in wanted:
        if ean in db.rows:
            continue
        a = attempts.setdefault(ean, {"n": 0, "last": ""})
        if a["last"] == today:
            continue
        if a["n"] >= cfg.get("max_attempts", 7):
            not_found.append(ean)
            continue
        if not any(budgets.get(n, 0) > 0 for n, _ in LOOKUP_PROVIDERS):
            break
        a["n"] += 1
        a["last"] = today
        for name, fn in LOOKUP_PROVIDERS:
            if budgets.get(name, 0) <= 0:
                continue
            budgets[name] -= 1
            try:
                desc = fn(ean)
            except PermissionError as e:
                log(f"{name} stopped for today: {e}")
                budgets[name] = 0
                continue
            except Exception as e:
                log(f"{name} error for {ean}: {e}")
                continue
            if desc and db.upsert(ean, desc):
                log(f"Found {ean} via {name}: {desc}")
                break

    # tidy the inbox: remove found / given-up barcodes
    remaining = [e for e in wanted if e not in db.rows and e not in not_found]
    if inbox_files:
        for f in inbox_files[1:]:
            f.unlink()
        inbox_files[0].write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")
    if not_found:
        nf = INBOX / "not_found.txt"
        old = set(nf.read_text().split()) if nf.exists() else set()
        nf.write_text("\n".join(sorted(old | set(not_found))) + "\n")
    st["attempts"] = {e: v for e, v in attempts.items() if e in remaining}
    log(f"Lookup queue: {len(remaining)} waiting, {len(not_found)} given up")
    return True


# --- Open Food / Beauty / Products / Pet Food Facts

OPENFACTS_SITES = {
    "food":     ("https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz", "csv",
                 "https://static.openfoodfacts.org/data/delta/"),
    "beauty":   ("https://static.openbeautyfacts.org/data/openbeautyfacts-products.jsonl.gz", "jsonl",
                 "https://static.openbeautyfacts.org/data/delta/"),
    "products": ("https://static.openproductsfacts.org/data/openproductsfacts-products.jsonl.gz", "jsonl",
                 "https://static.openproductsfacts.org/data/delta/"),
    "petfood":  ("https://static.openpetfoodfacts.org/data/openpetfoodfacts-products.jsonl.gz", "jsonl",
                 "https://static.openpetfoodfacts.org/data/delta/"),
}


def openfacts_desc(p):
    name = p.get("product_name") or p.get("product_name_en") or p.get("generic_name")
    return join_desc(p.get("brands"), name, p.get("quantity"))


def src_openfacts(db, st, cfg):
    ok = True
    seeded = st.setdefault("seeded", {})
    done = set(st.get("processed_deltas", []))
    for site, (seed_url, fmt, delta_base) in OPENFACTS_SITES.items():
        if not seeded.get(site):
            log(f"Seeding Open Facts '{site}' ...")
            try:
                lines = stream_gz_lines(seed_url)
                if fmt == "csv":
                    for row in csv.DictReader(lines, delimiter="\t", quoting=csv.QUOTE_NONE):
                        db.upsert(row.get("code"), openfacts_desc(row))
                else:
                    for line in lines:
                        try:
                            p = json.loads(line)
                        except ValueError:
                            continue
                        db.upsert(p.get("code"), openfacts_desc(p))
                seeded[site] = db.ts
            except Exception as e:
                log(f"Seed '{site}' failed (will retry next run): {e}")
                ok = False
            db.save()
        try:
            r = HTTP.get(delta_base + "index.txt", timeout=60)
            r.raise_for_status()
        except Exception as e:
            log(f"No deltas for '{site}': {e}")
            continue
        for name in sorted(l.strip() for l in r.text.splitlines() if l.strip().endswith(".json.gz")):
            url = delta_base + name
            if url in done:
                continue
            try:
                for line in stream_gz_lines(url):
                    try:
                        p = json.loads(line)
                    except ValueError:
                        continue
                    db.upsert(p.get("code"), openfacts_desc(p))
                done.add(url)
            except Exception as e:
                log(f"Delta failed {url}: {e}")
    st["processed_deltas"] = sorted(done)[-2000:]
    return ok


# --- Wikidata

def src_wikidata(db, st, cfg):
    query = """
    SELECT ?gtin ?label ?brandLabel WHERE {
      ?item wdt:P3962 ?gtin .
      ?item rdfs:label ?label . FILTER(LANG(?label) = "en")
      OPTIONAL { ?item wdt:P1716 ?brand . ?brand rdfs:label ?brandLabel . FILTER(LANG(?brandLabel) = "en") }
    }"""
    r = HTTP.post("https://query.wikidata.org/sparql", data={"query": query},
                  headers={"Accept": "application/sparql-results+json"}, timeout=300)
    r.raise_for_status()
    rows = r.json()["results"]["bindings"]
    for b in rows:
        db.upsert(b["gtin"]["value"],
                  join_desc(b.get("brandLabel", {}).get("value"), b["label"]["value"]))
    log(f"Wikidata returned {len(rows):,} rows")
    return True


# --- USDA FoodData Central (branded foods)

def src_usda(db, st, cfg):
    url = cfg.get("zip_url")
    if not url:
        page = HTTP.get("https://fdc.nal.usda.gov/download-datasets", timeout=60).text
        found = re.findall(r'[^"\'\s>]*FoodData_Central_branded_food_csv_[\d-]+\.zip', page)
        if not found:
            log("USDA: download link not found - set sources.usda.zip_url in config.json")
            return False
        url = urljoin("https://fdc.nal.usda.gov/", sorted(found)[-1])
    if st.get("last_url") == url:
        log("USDA: no new release")
        return True
    log(f"USDA: downloading {url}")
    path = download_to_temp(url, ".zip")
    try:
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            food_name = next(n for n in names if n.endswith("/food.csv") or n == "food.csv")
            branded_name = next(n for n in names if n.endswith("branded_food.csv"))
            descriptions = {}
            with z.open(food_name) as fh:
                for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace")):
                    if row.get("data_type") == "branded_food":
                        descriptions[row["fdc_id"]] = row.get("description", "")
            with z.open(branded_name) as fh:
                for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace")):
                    name = fix_case(descriptions.get(row.get("fdc_id"), ""))
                    brand = fix_case(row.get("brand_name") or row.get("brand_owner") or "")
                    db.upsert(row.get("gtin_upc"), join_desc(brand, name, row.get("package_weight")))
    finally:
        os.remove(path)
    st["last_url"] = url
    return True


# --- Discogs (music)

def latest_discogs_url():
    keys = []
    year = datetime.now(timezone.utc).year
    for y in (year, year - 1):
        r = HTTP.get("https://discogs-data-dumps.s3.us-west-2.amazonaws.com/",
                     params={"prefix": f"data/{y}/"}, timeout=60)
        if r.ok:
            keys += re.findall(r"<Key>(data/\d{4}/discogs_\d{8}_releases\.xml\.gz)</Key>", r.text)
        if keys:
            break
    return "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/" + sorted(keys)[-1] if keys else None


def parse_discogs(binary_stream, db):
    context = ET.iterparse(binary_stream, events=("start", "end"))
    _, root = next(context)
    for event, el in context:
        if event != "end" or el.tag != "release":
            continue
        barcodes = [i.get("value") for i in el.findall("identifiers/identifier")
                    if (i.get("type") or "").lower() == "barcode"]
        if barcodes:
            artist = el.findtext("artists/artist/name") or ""
            artist = re.sub(r"\s\(\d+\)$", "", artist)
            title = el.findtext("title") or ""
            fmt_el = el.find("formats/format")
            fmt = fmt_el.get("name") if fmt_el is not None else ""
            desc = f"{artist} - {title}" if artist else title
            if fmt:
                desc += f" ({fmt})"
            for bc in barcodes:
                db.upsert(bc, desc)
        root.clear()


def src_discogs(db, st, cfg):
    url = latest_discogs_url()
    if not url:
        log("Discogs: no dump found")
        return False
    if st.get("last_url") == url:
        log("Discogs: no new dump")
        return True
    log(f"Discogs: {url}")
    r = HTTP.get(url, stream=True, timeout=(30, 600))
    r.raise_for_status()
    parse_discogs(gzip.GzipFile(fileobj=r.raw), db)
    st["last_url"] = url
    return True


# --- Open Library (books)

def parse_openlibrary_line(line, db):
    if "isbn_" not in line:
        return
    try:
        rec = json.loads(line.rsplit("\t", 1)[-1])
    except ValueError:
        return
    title = clean(rec.get("title"))
    if not title:
        return
    if rec.get("subtitle"):
        title += ": " + clean(rec["subtitle"])
    extra = clean(rec.get("physical_format"))
    pubs = rec.get("publishers") or []
    desc = join_desc("", title, f"({extra})" if extra else "")
    if pubs:
        desc += f" - {clean(pubs[0])}"
    codes = list(rec.get("isbn_13") or []) + [isbn10_to_ean13(i) for i in rec.get("isbn_10") or []]
    for c in codes:
        if c:
            db.upsert(c, desc)


def src_openlibrary(db, st, cfg):
    log("Open Library: streaming editions dump (large)")
    for i, line in enumerate(stream_gz_lines("https://openlibrary.org/data/ol_dump_editions_latest.txt.gz"), 1):
        parse_openlibrary_line(line, db)
        if i % 2_000_000 == 0:
            log(f"  {i:,} lines")
    return True


# --- Web Data Commons (schema.org Product from Common Crawl)

NQ = re.compile(r'^(\S+)\s+<([^>]+)>\s+(.+?)\s+<([^>]*)>\s*\.\s*$')
LIT = re.compile(r'^"((?:[^"\\]|\\.)*)"')
ESC = re.compile(r'\\(u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}|.)')

WDC_ID_PROPS = ("gtin13", "gtin12", "gtin14", "gtin8", "gtin")
WDC_EXTRA_ID_PROPS = ("sku", "productID")     # only trusted when matched against the wanted list
WDC_QUICK = ("/name>", "gtin", "/sku>", "/productID>", "#type>")


def nq_unescape(s):
    def rep(m):
        t = m.group(1)
        if t[0] in "uU":
            try:
                return chr(int(t[1:], 16))
            except ValueError:
                return ""
        return {"n": " ", "t": " ", "r": " "}.get(t, t)
    return ESC.sub(rep, s)


def load_wanted():
    """Barcodes you still need descriptions for: inbox/wanted*.txt (one per line)."""
    wanted = set()
    for f in sorted(INBOX.glob("wanted*.txt")):
        for token in f.read_text(encoding="utf-8", errors="replace").split():
            ean = to_ean13(token, pad_short=True)
            if ean:
                wanted.add(ean)
    return wanted


def tidy_wanted(db):
    """Remove barcodes that now have a description from the wanted files."""
    files = sorted(INBOX.glob("wanted*.txt"))
    left = 0
    for f in files:
        keep = []
        for token in f.read_text(encoding="utf-8", errors="replace").split():
            ean = to_ean13(token, pad_short=True)
            if ean and ean not in db.rows:
                keep.append(ean)
        f.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")
        left += len(keep)
    return left


class WdcStop(Exception):
    pass


def parse_nquads(lines, db, wanted=None, skip_lines=0, progress=None):
    """Collect product names and barcodes per web page (graph) and upsert matches."""
    pages = OrderedDict()   # graph -> {subject: node}
    count = 0

    def flush(page):
        product_names = [n["name"] for n in page.values() if n.get("product") and n.get("name")]
        fallback = product_names[0] if len(set(product_names)) == 1 else None
        for node in page.values():
            ids = node.get("ids")
            if not ids:
                continue
            name = node.get("name") or fallback
            if not name:
                continue
            for prop, value in ids:
                ean = to_ean13(value)
                if not ean:
                    continue
                if wanted is not None:
                    if ean not in wanted:
                        continue
                elif prop in WDC_EXTRA_ID_PROPS:
                    continue
                if db.upsert(ean, name) and wanted is not None:
                    wanted.discard(ean)
                    progress["found"] += 1
                break

    for line in lines:
        count += 1
        if count <= skip_lines:
            continue
        if count % 2_000_000 == 0:
            if progress is not None:
                progress["line"] = count
                log(f"  {count:,} lines, found {progress['found']:,}")
            if minutes_used() > CFG["max_runtime_minutes"]:
                for page in pages.values():
                    flush(page)
                raise WdcStop(count)
        if not any(q in line for q in WDC_QUICK):
            continue
        m = NQ.match(line)
        if not m:
            continue
        subj, pred, obj, graph = m.groups()
        prop = pred.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

        page = pages.get(graph)
        if page is None:
            page = pages[graph] = {}
            if len(pages) > 500:
                flush(pages.popitem(last=False)[1])

        if prop == "type":
            if obj.rstrip(">").rsplit("/", 1)[-1] == "Product":
                page.setdefault(subj, {})["product"] = True
            continue
        if prop not in WDC_ID_PROPS and prop not in WDC_EXTRA_ID_PROPS and prop != "name":
            continue
        lit = LIT.match(obj)
        if not lit:
            continue
        value = nq_unescape(lit.group(1))
        node = page.setdefault(subj, {})
        if prop == "name":
            node.setdefault("name", value)
        else:
            node.setdefault("ids", []).append((prop, value))

    for page in pages.values():
        flush(page)
    return count


def wdc_part_urls(cfg):
    """Accepts direct .gz links, file lists (one link per line) or a directory/HTML page with .gz links."""
    sources = list(cfg.get("file_list_urls") or [])
    if cfg.get("file_list_url"):
        sources.append(cfg["file_list_url"])
    parts = []

    def add(url):
        if url not in parts:
            parts.append(url)

    for src in sources:
        src = src.strip()
        if not src:
            continue
        if src.split("?")[0].endswith(".gz"):
            add(src)
            continue
        r = HTTP.get(src, timeout=120)
        r.raise_for_status()
        text = r.text
        links = re.findall(r'href="([^"]+\.gz)"', text, re.I) if "<a " in text.lower() else \
            [l.strip() for l in text.splitlines() if l.strip().endswith(".gz")]
        for link in links:
            add(urljoin(src, html.unescape(link)))
    return parts


def src_webdatacommons(db, st, cfg):
    if not (cfg.get("file_list_urls") or cfg.get("file_list_url")):
        log("Web Data Commons: set sources.webdatacommons.file_list_urls in config.json")
        return False

    wanted = None
    if cfg.get("wanted_only", True):
        wanted = load_wanted()
        wanted -= set(db.rows)
        log(f"Wanted barcodes still missing: {len(wanted):,}")
        if not wanted:
            log("Nothing wanted - add barcodes to inbox/wanted_barcodes.txt")
            return True

    parts = wdc_part_urls(cfg)
    done = set(st.get("done", []))
    todo = [p for p in parts if p not in done][: cfg.get("parts_per_run", 50)]
    partial = st.get("partial") or {}
    progress = {"found": 0, "line": 0}

    for url in todo:
        if minutes_used() > CFG["max_runtime_minutes"]:
            break
        skip = partial.get("line", 0) if partial.get("url") == url else 0
        log(f"WDC part: {url}" + (f" (resuming after line {skip:,})" if skip else ""))
        try:
            parse_nquads(stream_gz_lines(url), db, wanted, skip, progress)
            done.add(url)
            st["done"] = sorted(done)
            st.pop("partial", None)
            partial = {}
        except WdcStop as stop:
            st["partial"] = {"url": url, "line": int(str(stop))}
            log(f"Time budget used - will resume this part next run")
            db.save()
            break
        except Exception as e:
            log(f"WDC part failed: {e}")
        db.save()
        if wanted is not None and not wanted:
            break

    st["found_total"] = st.get("found_total", 0) + progress["found"]
    left = tidy_wanted(db)
    log(f"WDC progress: {len(done & set(parts))}/{len(parts)} parts, found this run {progress['found']:,}, wanted left {left:,}")
    return True


SOURCES = OrderedDict([
    ("item_master", src_item_master),
    ("lookup_apis", src_lookup_apis),
    ("openfacts", src_openfacts),
    ("wikidata", src_wikidata),
    ("usda", src_usda),
    ("discogs", src_discogs),
    ("openlibrary", src_openlibrary),
    ("webdatacommons", src_webdatacommons),
])


# ------------------------------------------------------------------ main

def is_due(st, cfg):
    days = cfg.get("every_days", 0)
    last = st.get("last_success")
    if not days or not last:
        return True
    return datetime.now(timezone.utc) - datetime.fromisoformat(last.replace("Z", "+00:00")) >= timedelta(days=days) - timedelta(hours=2)


def main():
    forced = None
    if "--sources" in sys.argv:
        i = sys.argv.index("--sources")
        forced = {s.strip() for s in (sys.argv[i + 1] if i + 1 < len(sys.argv) else "").split(",") if s.strip()}
        forced = forced or None

    state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}

    def save_state():
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
        os.replace(tmp, STATE_FILE)

    db = Database()
    db.load()

    for name, fn in SOURCES.items():
        cfg = CFG["sources"].get(name, {})
        st = state.setdefault("sources", {}).setdefault(name, {})
        if forced is not None:
            if name not in forced:
                continue
        elif not cfg.get("enabled") or not is_due(st, cfg):
            continue
        if minutes_used() > CFG["max_runtime_minutes"]:
            log(f"Time budget used - '{name}' postponed to next run")
            continue
        log(f"=== {name} ===")
        db.begin(name)
        try:
            if fn(db, st, cfg):
                st["last_success"] = db.ts
        except Exception:
            traceback.print_exc()
        st["last_result"] = db.stats[name]
        log(f"{name}: {db.stats[name]}")
        db.seen = None
        db.save()
        save_state()

    state["last_run"] = db.ts
    state["total_records"] = len(db.rows)
    state["records_by_source"] = dict(Counter(r[3] for r in db.rows.values()).most_common())
    db.save()
    save_state()
    log(f"Done. Total records: {len(db.rows):,}")


if __name__ == "__main__":
    main()
