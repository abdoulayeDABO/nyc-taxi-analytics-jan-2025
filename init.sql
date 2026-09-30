CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================
-- DIMENSIONS
-- ============================================

CREATE TABLE dwh.dim_zone (
    location_id     INT PRIMARY KEY,
    borough         VARCHAR(50),
    zone            VARCHAR(100),
    service_zone    VARCHAR(50)
);

CREATE TABLE dwh.dim_vendor (
    vendor_id       INT PRIMARY KEY,
    vendor_name     VARCHAR(50)
);

CREATE TABLE dwh.dim_ratecode (
    ratecode_id     INT PRIMARY KEY,
    ratecode_desc   VARCHAR(50)
);

CREATE TABLE dwh.dim_payment_type (
    payment_type_id INT PRIMARY KEY,
    payment_desc    VARCHAR(50)
);

CREATE TABLE dwh.dim_date (
    date_id           INT PRIMARY KEY,        -- YYYYMMDD
    full_date         DATE,
    day               INT,
    month             INT,
    year              INT,
    quarter           INT,
    day_of_week       INT,
    day_name          VARCHAR(10),
    is_weekend        BOOLEAN,
    day_of_year       INT,
    week_of_year      INT,
    month_name        VARCHAR(10),
    year_month        VARCHAR(7),             -- 2025-01
    year_quarter      VARCHAR(7),             -- 2025-Q1
    season            VARCHAR(10),
    is_month_start    BOOLEAN,
    is_month_end      BOOLEAN
);

CREATE TABLE dwh.dim_date (
    date_id           INT PRIMARY KEY,        -- YYYYMMDD
    full_date         DATE,
    day               INT,
    month             INT,
    year              INT,
    quarter           INT,
    day_of_week       INT,                    -- 1=Lundi ... 7=Dimanche
    day_name          VARCHAR(20),            
    is_weekend        BOOLEAN,
    day_of_year       INT,
    week_of_year      INT,
    month_name        VARCHAR(20),            
    year_month        VARCHAR(7),             -- 2025-01
    year_quarter      VARCHAR(7),             
    season            VARCHAR(20),
    is_month_start    BOOLEAN,
    is_month_end      BOOLEAN
);

CREATE TABLE dwh.dim_time (
    hour_id         INT PRIMARY KEY,          -- 0 à 23
    period          VARCHAR(20),              -- Nuit / Matin / Après-midi / Soir
    hour_label      VARCHAR(11),              -- 08:00-08:59
    hour_12         INT,
    am_pm           VARCHAR(2),
    is_night        BOOLEAN,
    is_rush_hour    BOOLEAN,
    rush_type       VARCHAR(15)               -- Pointe matin / Pointe soir / Hors pointe
);

-- ============================================
-- TABLE DE FAITS (grain : jour x heure x zone départ x zone arrivée x paiement x tarif x vendor)
-- ============================================

CREATE TABLE dwh.fact_trips (
    date_id             INT REFERENCES dwh.dim_date(date_id),
    hour_id             INT REFERENCES dwh.dim_time(hour_id),
    pu_location_id      INT REFERENCES dwh.dim_zone(location_id),
    do_location_id      INT REFERENCES dwh.dim_zone(location_id),
    vendor_id           INT REFERENCES dwh.dim_vendor(vendor_id),
    payment_type_id     INT REFERENCES dwh.dim_payment_type(payment_type_id),
    ratecode_id         INT REFERENCES dwh.dim_ratecode(ratecode_id),

    nb_trips            BIGINT,
    total_passengers    BIGINT,
    total_distance      DOUBLE PRECISION,
    total_duration_sec  BIGINT,
    total_fare          DOUBLE PRECISION,
    total_tip           DOUBLE PRECISION,
    total_revenue       DOUBLE PRECISION,

    PRIMARY KEY (date_id, hour_id, pu_location_id, do_location_id,
                 vendor_id, payment_type_id, ratecode_id)
);

CREATE INDEX idx_fth_date   ON dwh.fact_trips(date_id);
CREATE INDEX idx_fth_hour   ON dwh.fact_trips(hour_id);
CREATE INDEX idx_fth_pu     ON dwh.fact_trips(pu_location_id);
CREATE INDEX idx_fth_do     ON dwh.fact_trips(do_location_id);
CREATE INDEX idx_fth_vendor ON dwh.fact_trips(vendor_id);