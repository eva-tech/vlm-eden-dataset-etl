-- 
-- depends: 20230207_01_53Bud

CREATE TABLE dim_technicians
(
    id         serial       not null primary key,
    name       varchar(255) not null unique,
    created_at timestamptz,
    updated_at timestamptz
);

ALTER TABLE fact_studies
    ADD COLUMN technicians_id int;

ALTER TABLE fact_studies ADD CONSTRAINT fk_technicians
        FOREIGN KEY (technicians_id)
            REFERENCES dim_technicians (id);
