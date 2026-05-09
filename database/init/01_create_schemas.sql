-- =========================
-- SCHEMAS
-- =========================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS inference;

-- =========================
-- RAW DATA TABLE
-- =========================
CREATE TABLE IF NOT EXISTS raw.diabetes_raw (
    id SERIAL PRIMARY KEY,
    patient_id TEXT,
    encounter_id TEXT,
    
    -- metadata
    batch_id TEXT NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT DEFAULT 'diabetic_data.csv',
    status TEXT DEFAULT 'loaded',
    row_hash TEXT,

    -- raw json (flexible ingestion)
    data JSONB
);

-- index for batch tracking
CREATE INDEX IF NOT EXISTS idx_raw_batch_id ON raw.diabetes_raw(batch_id);

-- =========================
-- CLEAN DATA TABLE
-- =========================
CREATE TABLE IF NOT EXISTS clean.diabetes_clean (
    id SERIAL PRIMARY KEY,

    patient_id TEXT,
    encounter_id TEXT,

    -- example engineered features
    age INTEGER,
    time_in_hospital INTEGER,
    num_lab_procedures INTEGER,
    num_medications INTEGER,

    -- target
    readmitted TEXT,

    -- metadata
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- INFERENCE LOGS
-- =========================
CREATE TABLE IF NOT EXISTS inference.inference_logs (
    id SERIAL PRIMARY KEY,

    request_id TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    input_data JSONB,
    prediction TEXT,
    probability FLOAT,

    model_name TEXT,
    model_version TEXT,

    response_time_ms FLOAT
);

-- index for monitoring
CREATE INDEX IF NOT EXISTS idx_inference_timestamp 
ON inference.inference_logs(timestamp);