# MLE Take-Home: Merchant Underwriting Pipeline

An end-to-end data pipeline that ingests merchant data from multiple sources, trains a dispute prediction model, and generates an AI-powered underwriting report.

## Repo Structure

```
.
├── docker-compose.yml          # Defines mock-api, pipeline, and test services
├── .env                        # Environment variables (API keys, base URLs)
│
├── data/                       # Shared data volume (CSV input, SQLite DB, outputs)
│   ├── merchants.csv           # Source merchant data
│   ├── merchants.db            # SQLite database (created by pipeline)
│   ├── model.joblib            # Trained model (created by pipeline)
│   ├── scrape_clarity_pay.json # ClarityPay scrape output
│   ├── underwriting_report.md  # Generated underwriting report
│   └── sample_merchant_summary.pdf
│
├── mock-api/                   # Internal Merchant Risk API
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # FastAPI app serving /merchant-risk/{id}
│
├── src/                        # Pipeline source code
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── run.py                  # Top-level pipeline orchestrator
│   │
│   ├── ingestion/              # Data ingestion scripts
│   │   ├── ingest_merchants_csv.py       # Load and validate merchants.csv
│   │   ├── ingest_rest_countries.py      # Fetch country data from REST Countries API
│   │   ├── ingest_companies_house.py     # Fetch UK company data from Companies House API
│   │   ├── ingest_merchant_risk_api.py   # Fetch risk flags from internal mock API
│   │   ├── ingest_merchant_summary_pdf.py # Extract data from merchant summary PDF
│   │   └── ingest_clarity_pay.py         # Scrape ClarityPay homepage
│   │
│   ├── model/                  # Feature engineering and model training
│   │   ├── build_features.py   # Join tables, one-hot encode, compute features
│   │   └── train_model.py      # Train Random Forest to predict disputes
│   │
│   └── reporting/              # Report generation
│       └── portfolio_underwriting_report.py  # Generate underwriting report via Gemini
│
└── tests/                      # Test suite
    ├── ingestion/
    │   ├── test_ingest_merchants_csv.py
    │   ├── test_ingest_rest_countries.py
    │   ├── test_ingest_companies_house.py
    │   ├── test_ingest_merchant_risk_api.py
    │   ├── test_ingest_merchant_summary_pdf.py
    │   └── test_ingest_clarity_pay.py
    ├── model/
    │   └── test_train_model.py
    └── reporting/
        └── test_portfolio_underwriting_report.py
```

## Pipeline Steps

The pipeline runs automatically when the `pipeline` container starts (`src/run.py`):

### 1. Ingestion (sequential)
1. **Merchants CSV** -- Validates rows with Pydantic and loads into SQLite. Adds a `month` field (1st of current month). Unique on `(month, merchant_id)`.
2. **REST Countries** -- Fetches country metadata for each unique country in the merchants table. Incremental: skips countries already ingested.
3. **Companies House** -- Fetches UK company profiles by registration number. Requires `COMPANIES_HOUSE_API_KEY`. Handles 429 rate limits with a 5-minute wait.
4. **Merchant Risk API** -- Calls the internal mock API for each merchant to retrieve risk flags and transaction summaries.

### 2. Ingestion (concurrent)
5. **Merchant Summary PDF** -- Extracts structured data from `sample_merchant_summary.pdf` using pdfplumber (async).
6. **ClarityPay Scrape** -- Scrapes claritypay.com for stats, proud partners, and value propositions. Outputs `scrape_clarity_pay.json`.

### 3. Model Training
7. **Build Features** -- Joins merchants and countries tables. Computes `avg_ticket_size`. One-hot encodes categoricals. Expands list columns (currencies, languages) into binary features.
8. **Train Model** -- Trains a Random Forest regressor (70/30 split) to predict `dispute_count`. Saves model to `data/model.joblib`. Logs R2, MAE, RMSE for train and test sets.

### 4. Reporting
9. **Underwriting Report** -- Aggregates portfolio stats (merchant count, volume, dispute rates by country and risk level). Sends to Gemini to generate a markdown report saved to `data/underwriting_report.md`.

## Environment Variables

| Variable | Description |
|---|---|
| `DATA_DIR` | Path to the data directory (default: `data`) |
| `REST_COUNTRIES_BASE_URL` | REST Countries API base URL |
| `COMPANIES_HOUSE_BASE_URL` | Companies House API base URL |
| `COMPANIES_HOUSE_API_KEY` | Companies House API key |
| `MERCHANT_RISK_API_BASE_URL` | Internal mock API URL (default: `http://mock-api:8000`) |
| `GEMINI_API_KEY` | Google Gemini API key for report generation |

## Commands

### Start all services and run the pipeline

```bash
docker compose up --build
```

This starts the mock API and runs the full pipeline.

### Run the pipeline manually

```bash
docker compose run --rm pipeline
```

### Run individual pipeline steps

```bash
docker compose run --rm pipeline python -m ingestion.ingest_merchants_csv
docker compose run --rm pipeline python -m ingestion.ingest_rest_countries
docker compose run --rm pipeline python -m ingestion.ingest_companies_house
docker compose run --rm pipeline python -m ingestion.ingest_merchant_risk_api
docker compose run --rm pipeline python -m ingestion.ingest_merchant_summary_pdf
docker compose run --rm pipeline python -m ingestion.ingest_clarity_pay
docker compose run --rm pipeline python -m model.train_model
docker compose run --rm pipeline python -m reporting.portfolio_underwriting_report
```

### Run all tests

```bash
docker compose run --rm test
```
