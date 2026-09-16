# Retail EAN-13 Database

Four-column product database, rebuilt and updated automatically every day by GitHub Actions.

| File | Content |
|---|---|
| `data/ean_<prefix>.csv` | `ean13, description, created_at, modified_at` (UTC) |
| `meta/src_<prefix>.csv` | which source supplied each record |
| `state.json` | progress, totals and per-source results |
| `inbox/missing_barcodes.txt` | barcodes to look up via APIs (one per line) |
| `inbox/item_master*.csv` | your own item master (barcode + description columns) |

Files are split by barcode prefix (3 digits, 5 digits for books 978/979) so no file exceeds GitHub limits.

## Admin web app (`admin/index.html`)
Search by barcode, description, created or modified date; edit, add and delete records; find and replace;
import CSV/Excel; export search results or the whole database to CSV/Excel.
Your edits and imports are saved as source `manual` (priority 1000), so the daily job never overwrites them.
Deleted barcodes are listed in `meta/deleted.csv` and are never re-added automatically.

## Sources and trust order
manual (1000) > item_master (100) > usda (80) > openfacts (70) > lookup_apis (60) > wikidata (50) > discogs / openlibrary (40) > webdatacommons (20)

A source can replace a description from an equal or lower source; lower sources only fill gaps.
Enable, disable or re-rank sources in `config.json`.

## Attribution
Contains data from Open Food Facts, Open Beauty Facts, Open Products Facts and Open Pet Food Facts (ODbL),
USDA FoodData Central (public domain), Wikidata (CC0), Discogs (CC0), Open Library and Web Data Commons.
