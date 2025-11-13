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


DROP VIEW studies_uploaded_by_date;

CREATE VIEW studies_uploaded_by_date AS
select count(fs.id), dc.date_actual, fs.modality_id, fs.facility_id, fs.radiologist_technician_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_id
where not fs.deleted
group by dc.date_dim_id, fs.modality_id, fs.facility_id, fs.radiologist_technician_id;
