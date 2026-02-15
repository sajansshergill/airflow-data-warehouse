CREATE TABLE IF NOT EXISTS dim_crypto_asset (
    asset_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_crypto_price_hourly (
    asset_id TEXT NOT NULL REFERENCES dim_crypto_asset(asset_id),
    ts TIMESTAMP NOT NULL,
    price_usd NUMERIC(18, 8) NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset_id, ts)
);

-- Optional pipeline state (watermark)
CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_success_ts TIMESTAMPTZ,
    update_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);