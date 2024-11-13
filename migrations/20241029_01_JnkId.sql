-- 
-- depends: 20230627_01_81uau

ALTER TABLE fact_studies
    DROP CONSTRAINT fk_technicians;

ALTER TABLE fact_studies
    DROP COLUMN technicians_id;

ALTER TABLE fact_studies
    ADD COLUMN radiologist_technician_id int;

ALTER TABLE fact_studies
    add CONSTRAINT fk_radiologist_technician_id
        FOREIGN KEY (radiologist_technician_id)
            REFERENCES dim_practitioners (id)
            ;