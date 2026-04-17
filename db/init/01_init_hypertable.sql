-- =============================================================================
-- TSSI – TimescaleDB bootstrap
-- Loaded automatically by the timescale/timescaledb image from
-- /docker-entrypoint-initdb.d on first container start.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------------------------------------
-- Raw observations table.
--
-- Notes:
--   * `ppu` is a STORED generated column so the database itself enforces the
--     Price-Per-Unit invariant (nominal_price / net_weight). Application code
--     never writes to this column.
--   * A composite primary key that includes the partitioning column (`time`)
--     is required by TimescaleDB hypertables.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tssi_raw_data (
    time              TIMESTAMPTZ     NOT NULL,
    asset_name        VARCHAR(64)     NOT NULL,
    platform_source   VARCHAR(64)     NOT NULL,
    nominal_price     NUMERIC(12, 4)  NOT NULL CHECK (nominal_price >= 0),
    net_weight        NUMERIC(12, 4)  NOT NULL CHECK (net_weight > 0),
    unit_type         VARCHAR(4)      NOT NULL CHECK (unit_type IN ('g', 'ml')),
    ppu               NUMERIC(18, 6)  GENERATED ALWAYS AS
                        (nominal_price / NULLIF(net_weight, 0)) STORED,
    PRIMARY KEY (time, asset_name, platform_source)
);

-- Convert to hypertable (chunked by day-ish intervals; default 7 days is fine
-- for a low-frequency daily ingestion workload).
SELECT create_hypertable(
    'tssi_raw_data',
    'time',
    if_not_exists => TRUE,
    migrate_data  => TRUE
);

-- Query accelerators
CREATE INDEX IF NOT EXISTS idx_tssi_asset_time
    ON tssi_raw_data (asset_name, time DESC);

CREATE INDEX IF NOT EXISTS idx_tssi_platform_time
    ON tssi_raw_data (platform_source, time DESC);

-- -----------------------------------------------------------------------------
-- Continuous aggregate: daily median PPU per asset.
-- Used by the calculation engine (Phase 4) as the canonical daily PPU feed,
-- de-duplicating multi-platform observations into a single per-asset value.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS tssi_daily_ppu
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    asset_name,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY ppu) AS median_ppu,
    AVG(ppu)  AS mean_ppu,
    COUNT(*)  AS sample_count
FROM tssi_raw_data
GROUP BY bucket, asset_name
WITH NO DATA;

-- Refresh policy: keep the last 30 days hot, refresh hourly.
SELECT add_continuous_aggregate_policy(
    'tssi_daily_ppu',
    start_offset      => INTERVAL '30 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists     => TRUE
);
