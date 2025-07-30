-- setup_database.sql (Corrected)

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    plan TEXT DEFAULT 'trial',
    trial_end_date DATE,
    stripe_customer_id TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS foundations (
    ein TEXT PRIMARY KEY,
    name TEXT,
    address_line_1 TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    assets_fmv BIGINT,
    mission_statement TEXT
);

CREATE TABLE IF NOT EXISTS charities (
    ein TEXT PRIMARY KEY,
    name TEXT,
    city TEXT,
    state TEXT,
    address_line_1 TEXT,
    zip_code TEXT,
    mission_statement TEXT,
    normalized_name TEXT
);

CREATE TABLE IF NOT EXISTS charity_profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    charity_name TEXT,
    charity_ein TEXT,
    mission_statement TEXT
);

CREATE TABLE IF NOT EXISTS grants (
    id SERIAL PRIMARY KEY,
    foundation_ein TEXT REFERENCES foundations(ein) ON DELETE CASCADE,
    recipient_name TEXT,
    grant_amount NUMERIC,
    grant_purpose TEXT,
    tax_year INTEGER,
    recipient_ein TEXT,
    recipient_ein_matched TEXT,
    normalized_name TEXT, -- Comma was missing here
    embedding public.vector(384)
);
