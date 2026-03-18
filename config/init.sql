-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Create audit tables
CREATE TABLE IF NOT EXISTS public.dbt_tests (
    id SERIAL PRIMARY KEY,
    test_name VARCHAR(255),
    status VARCHAR(50),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA staging TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA marts TO datauser;