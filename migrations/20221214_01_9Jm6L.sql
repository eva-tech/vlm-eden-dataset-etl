-- 
-- depends:

CREATE TABLE dim_calendar
(
    date_dim_id            INT        NOT NULL,
    date_actual            DATE       NOT NULL,
    epoch                  BIGINT     NOT NULL,
    day_suffix             VARCHAR(4) NOT NULL,
    day_name               VARCHAR(9) NOT NULL,
    day_of_week            INT        NOT NULL,
    day_of_month           INT        NOT NULL,
    day_of_quarter         INT        NOT NULL,
    day_of_year            INT        NOT NULL,
    week_of_month          INT        NOT NULL,
    week_of_year           INT        NOT NULL,
    week_of_year_iso       CHAR(10)   NOT NULL,
    month_actual           INT        NOT NULL,
    month_name             VARCHAR(9) NOT NULL,
    month_name_abbreviated CHAR(3)    NOT NULL,
    quarter_actual         INT        NOT NULL,
    quarter_name           VARCHAR(9) NOT NULL,
    year_actual            INT        NOT NULL,
    first_day_of_week      DATE       NOT NULL,
    last_day_of_week       DATE       NOT NULL,
    first_day_of_month     DATE       NOT NULL,
    last_day_of_month      DATE       NOT NULL,
    first_day_of_quarter   DATE       NOT NULL,
    last_day_of_quarter    DATE       NOT NULL,
    first_day_of_year      DATE       NOT NULL,
    last_day_of_year       DATE       NOT NULL,
    mmyyyy                 CHAR(6)    NOT NULL,
    mmddyyyy               CHAR(10)   NOT NULL,
    weekend_indr           BOOLEAN    NOT NULL
);

ALTER TABLE dim_calendar
    ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX d_date_date_actual_idx
    ON dim_calendar (date_actual);

INSERT INTO dim_calendar
SELECT TO_CHAR(datum, 'yyyymmdd')::INT                                                        AS date_dim_id,
       datum                                                                                  AS date_actual,
       EXTRACT(EPOCH FROM datum)                                                              AS epoch,
       TO_CHAR(datum, 'fmDDth')                                                               AS day_suffix,
       TO_CHAR(datum, 'TMDay')                                                                AS day_name,
       EXTRACT(ISODOW FROM datum)                                                             AS day_of_week,
       EXTRACT(DAY FROM datum)                                                                AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1                                         AS day_of_quarter,
       EXTRACT(DOY FROM datum)                                                                AS day_of_year,
       TO_CHAR(datum, 'W')::INT                                                               AS week_of_month,
       EXTRACT(WEEK FROM datum)                                                               AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum)                                                              AS month_actual,
       TO_CHAR(datum, 'TMMonth')                                                              AS month_name,
       TO_CHAR(datum, 'Mon')                                                                  AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum)                                                            AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END                                                                                AS quarter_name,
       EXTRACT(YEAR FROM datum)                                                               AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT                                          AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT                                          AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT                                             AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE                        AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE                                                     AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE                      AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD')                            AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD')                            AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy')                                                               AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy')                                                             AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END                                                                                AS weekend_indr
FROM (SELECT '1999-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

create table dim_facilities
(
    id          serial       not null primary key,
    external_id uuid         not null unique,
    name        varchar(255) not null,
    address     text,
    timezone    varchar(100),
    country     varchar(100),
    state       varchar(100),
    city        varchar(100),
    created_at  timestamptz,
    updated_at  timestamptz
);

CREATE INDEX d_facilities_external_idx
    ON dim_facilities (external_id);

create table dim_modalities
(
    id          serial not null primary key,
    external_id uuid   not null unique,
    name        varchar(255),
    identifier  varchar(100) unique,
    description text,
    created_at  timestamptz,
    updated_at  timestamptz
);

CREATE INDEX d_modalities_external_idx
    ON dim_modalities (external_id);

CREATE INDEX d_modalities_identifier_idx
    ON dim_modalities (identifier);

create table dim_practitioners
(
    id            serial not null primary key,
    external_id   uuid   not null unique,
    name          varchar(255),
    first_surname varchar(255),
    last_surname  varchar(255),
    full_name     text,
    status        varchar(100),
    gender        varchar(255),
    created_at    timestamptz,
    updated_at    timestamptz
);

CREATE INDEX d_practitioner_external_idx
    ON dim_practitioners (external_id);

create table fact_studies
(
    id                          serial not null primary key,
    external_id                 uuid   not null unique,
    status                      varchar(31),
    sign_at                     timestamptz,
    sent_at                     timestamptz,
    patient_full_name           varchar(255),
    urgency_level               varchar(31),
    created_at                  timestamptz,
    updated_at                  timestamptz,
    dicom_date_time             timestamptz,
    facility_id                 int    not null,
    modality_id                 int    not null,
    calendar_id                 int    not null,
    practitioner_id             int    not null,
    patient_id                  uuid,
    gender                      varchar(10),
    birth_date                  timestamptz,
    referring_practitioner_id   int,
    signed_by_id                int,
    deleted                     boolean,
    migrated                    boolean,
    calendar_sign_at_id         int,
    calendar_sent_at_id         int,
    calendar_birth_date_id      int,
    calendar_dicom_date_time_id int,
    CONSTRAINT fk_facility
        FOREIGN KEY (facility_id)
            REFERENCES dim_facilities (id),
    CONSTRAINT fk_modality
        FOREIGN KEY (modality_id)
            REFERENCES dim_modalities (id),
    CONSTRAINT fk_calendar
        FOREIGN KEY (calendar_id)
            REFERENCES dim_calendar (date_dim_id),
    CONSTRAINT fk_sign_at_calendar
        FOREIGN KEY (calendar_sign_at_id)
            REFERENCES dim_calendar (date_dim_id),
    CONSTRAINT fk_sent_at_calendar
        FOREIGN KEY (calendar_sent_at_id)
            REFERENCES dim_calendar (date_dim_id),
    CONSTRAINT fk_birth_date_calendar
        FOREIGN KEY (calendar_birth_date_id)
            REFERENCES dim_calendar (date_dim_id),
    CONSTRAINT fk_dicom_date_time_calendar
        FOREIGN KEY (calendar_dicom_date_time_id)
            REFERENCES dim_calendar (date_dim_id),
    CONSTRAINT fk_practitioner
        FOREIGN KEY (practitioner_id)
            REFERENCES dim_practitioners (id),
    CONSTRAINT fk_referring_practitioner
        FOREIGN KEY (referring_practitioner_id)
            REFERENCES dim_practitioners (id),
    CONSTRAINT fk_signed_by_id
        FOREIGN KEY (signed_by_id)
            REFERENCES dim_practitioners (id)
);

CREATE INDEX f_studies_external_idx
    ON fact_studies (external_id);
