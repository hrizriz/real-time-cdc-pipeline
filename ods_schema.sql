-- ODS Database Schema (PostgreSQL)
-- Tabel untuk menyimpan data real-time dari CDC

-- Tabel customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    nik TEXT,
    full_name TEXT,
    date_of_birth DATE,
    gender TEXT,
    marital_status TEXT,
    phone_number TEXT,
    email TEXT,
    address TEXT,
    city TEXT,
    province TEXT,
    postal_code TEXT,
    occupation TEXT,
    employer_name TEXT,
    monthly_income NUMERIC(15,2),
    employment_status TEXT,
    years_of_employment INTEGER,
    education_level TEXT,
    emergency_contact_name TEXT,
    emergency_contact_phone TEXT,
    emergency_contact_relation TEXT,
    credit_score INTEGER,
    customer_segment TEXT,
    registration_date TIMESTAMP,
    last_updated TIMESTAMP,
    status TEXT,
    created_by TEXT,
    updated_by TEXT,
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_operation TEXT
);

CREATE INDEX IF NOT EXISTS idx_customers_nik ON customers (nik);
CREATE INDEX IF NOT EXISTS idx_customers_last_updated ON customers (last_updated);

-- Tabel credit_applications
CREATE TABLE IF NOT EXISTS credit_applications (
    application_id TEXT PRIMARY KEY,
    customer_id TEXT,
    application_date TIMESTAMP,
    vehicle_type TEXT,
    vehicle_brand TEXT,
    vehicle_model TEXT,
    vehicle_year INTEGER,
    vehicle_price NUMERIC(15,2),
    down_payment NUMERIC(15,2),
    loan_amount NUMERIC(15,2),
    tenor_months INTEGER,
    interest_rate NUMERIC(5,2),
    monthly_installment NUMERIC(15,2),
    application_status TEXT,
    approval_date TIMESTAMP,
    rejection_reason TEXT,
    disbursement_date TIMESTAMP,
    first_installment_date TIMESTAMP,
    last_payment_date TIMESTAMP,
    outstanding_amount NUMERIC(15,2),
    payment_status TEXT,
    collateral_status TEXT,
    notes TEXT,
    processed_by TEXT,
    approved_by TEXT,
    created_date TIMESTAMP,
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_operation TEXT
);

CREATE INDEX IF NOT EXISTS idx_credit_applications_customer_id ON credit_applications (customer_id);
CREATE INDEX IF NOT EXISTS idx_credit_applications_application_date ON credit_applications (application_date);

-- Tabel vehicle_ownership
CREATE TABLE IF NOT EXISTS vehicle_ownership (
    ownership_id TEXT PRIMARY KEY,
    customer_id TEXT,
    vehicle_type TEXT,
    brand TEXT,
    model TEXT,
    year INTEGER,
    vehicle_price NUMERIC(15,2),
    purchase_date DATE,
    ownership_status TEXT,
    registration_number TEXT,
    chassis_number TEXT,
    engine_number TEXT,
    created_date TIMESTAMP,
    cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_operation TEXT
);

CREATE INDEX IF NOT EXISTS idx_vehicle_ownership_customer_id ON vehicle_ownership (customer_id);
