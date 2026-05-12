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

CREATE TABLE IF NOT EXISTS processed.diabetes_processed (
    id BIGSERIAL PRIMARY KEY,

    encounter_id BIGINT,
    patient_nbr BIGINT,

    race VARCHAR(50),
    gender VARCHAR(20),
    age VARCHAR(20),

    time_in_hospital INTEGER,
    num_lab_procedures INTEGER,
    num_procedures INTEGER,
    num_medications INTEGER,
    number_outpatient INTEGER,
    number_emergency INTEGER,
    number_inpatient INTEGER,
    number_diagnoses INTEGER,

    diabetes_med VARCHAR(20),
    readmitted VARCHAR(20),

    batch_id VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.processed_batches (
    batch_id VARCHAR(100) PRIMARY KEY,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS feature_store.model_features (
    id BIGSERIAL PRIMARY KEY,

    encounter_id BIGINT,
    patient_nbr BIGINT,

    age_midpoint INTEGER,
    gender VARCHAR(20),
    race VARCHAR(50),

    time_in_hospital INTEGER,
    num_lab_procedures INTEGER,
    num_procedures INTEGER,
    num_medications INTEGER,
    number_outpatient INTEGER,
    number_emergency INTEGER,
    number_inpatient INTEGER,
    number_diagnoses INTEGER,

    total_previous_visits INTEGER,
    medication_intensity INTEGER,
    diabetes_med_flag INTEGER,

    readmitted_flag INTEGER,

    batch_id VARCHAR(100),
    feature_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.model_training_runs (
    id BIGSERIAL PRIMARY KEY,

    run_id VARCHAR(255),
    model_name VARCHAR(255),
    model_version VARCHAR(50),

    train_size INTEGER,
    test_size INTEGER,

    accuracy FLOAT,
    precision_score FLOAT,
    recall_score FLOAT,
    f1_score FLOAT,

    promoted_to_champion BOOLEAN DEFAULT FALSE,

    training_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.inference_logs (
    id BIGSERIAL PRIMARY KEY,

    request_id VARCHAR(255),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    input_data JSONB,
    prediction VARCHAR(50),
    probability FLOAT,

    model_name VARCHAR(255),
    model_alias VARCHAR(100),

    response_time_ms FLOAT
);