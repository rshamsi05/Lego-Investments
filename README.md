# Lego Star Wars Investment Analysis

An end-to-end data engineering pipeline that ingests, transforms, and models Lego Star Wars market data to support investment analysis. Built on a modern GCP stack with automated daily orchestration via Apache Airflow.

---

## High-Level Architecture
```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EXTRACT                                         │
│                                                                          │
│   ┌──────────────────────┐        ┌──────────────────────┐              │
│   │   Rebrickable API    │        │   Bricklink (Scraper)│              │
│   │                      │        │                      │              │
│   │  • Set catalog       │        │  • Price history     │              │
│   │  • Minifigures       │        │  • New/Used listings │              │
│   │  • Theme metadata    │        │  • Avg / Min / Max   │              │
│   └──────────┬───────────┘        └──────────┬───────────┘              │
│              │                               │                           │
└──────────────┼───────────────────────────────┼───────────────────────── ┘
               │                               │
               ▼                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          LOAD  →  DATA LAKE (GCS)                        │
│                                                                          │
│   gs://lego-investment-lake/rawFiles/                                    │
│   ├── rebrickable/                                                       │
│   │   ├── sets/YYYY-MM-DD.json                                           │
│   │   └── minifigures/{set_num}/YYYY-MM-DD.json                         │
│   └── bricklink/                                                         │
│       └── prices/{set_num}/YYYY-MM-DD.json                               │
│                                                                          │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     LOAD  →  BIGQUERY STAGING                            │
│                                                                          │
│   dataset: lego_staging                                                  │
│   ├── src_sets            (raw sets from Rebrickable)                    │
│   ├── src_minifigures     (raw minifigures from Rebrickable)             │
│   └── src_prices          (raw price observations from Bricklink)        │
│                                                                          │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     TRANSFORM  →  dbt MODELS                             │
│                                                                          │
│   Staging Layer                    Warehouse Layer                       │
│   ┌─────────────────┐              ┌─────────────────────────────────┐  │
│   │  stg_sets       │──────────┐   │  sets                           │  │
│   │  stg_minifigs   │──────────┼──▶│  • era_classification           │  │
│   │  stg_prices     │──────────┘   │  • price_per_piece              │  │
│   │  stg_set_minifs │              │  • price_appreciation_pct       │  │
│   └─────────────────┘              │  • annualized_return_pct        │  │
│                                    ├─────────────────────────────────┤  │
│                                    │  price_history                  │  │
│                                    │  • 7d / 30d / 90d rolling avg   │  │
│                                    │  • 30d price volatility         │  │
│                                    ├─────────────────────────────────┤  │
│                                    │  minifigures                    │  │
│                                    │  • set_count, is_exclusive      │  │
│                                    │  • first/last appearance        │  │
│                                    └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION  →  Apache Airflow                     │
│                                                                          │
│   DAG: lego_star_wars_pipeline   schedule: @daily                        │
│                                                                          │
│   extract_rebrickable ──┬──▶ load_sets ──────┐                          │
│                         └──▶ load_minifigs ──┼──▶ run_dbt               │
│   extract_bricklink ───────▶ load_prices ────┘                          │
│                                                                          │
│   • Retries: 1   • Retry delay: 5 min   • Catchup: disabled             │
└─────────────────────────────────────────────────────────────────────────┘
```
## Data Sources
### Rebrickable API
Provides the Lego Star Wars set catalog — set IDs, names, release years, piece counts, theme and sub-theme classifications, retail prices, retirement status, and per-set minifigure rosters. Authenticated via API key with paginated requests and a 0.2s rate limit per request.
### Bricklink (Scraped)
Provides secondary market price data — average, minimum, and maximum prices for new and used conditions over the last 6 months. Scraped via BeautifulSoup with randomized delays (4–10s) to avoid rate limiting.

## Airflow DAG
The lego_star_wars_pipeline DAG runs daily and implements the full ELT flow with explicit task dependencies:
```
extract_rebrickable ──┬──▶ load_sets ──────┐
                      └──▶ load_minifigs ──┼──▶ run_dbt
extract_bricklink ───────▶ load_prices ────┘
```

Rebrickable and Bricklink extraction run in parallel. Loading tasks are gated on their respective extraction completing. dbt transformation runs only after all three load tasks succeed.


---
*This project is built for educational and investment analysis purposes. Not affiliated with The LEGO Group.*
