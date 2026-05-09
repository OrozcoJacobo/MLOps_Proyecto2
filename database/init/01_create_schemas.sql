CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS feature_store;
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS raw.diabetes_raw (
    id BIGSERIAL PRIMARY KEY,
    encounter_id BIGINT,
    patient_nbr BIGINT,
    batch_id VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    row_hash VARCHAR(128),
    raw_data JSONB
);