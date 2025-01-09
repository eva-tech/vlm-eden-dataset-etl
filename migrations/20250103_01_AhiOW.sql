-- 
-- depends: 20241204_01_xSnrF

DROP VIEW studies_uploaded_by_date;

CREATE VIEW studies_uploaded_by_date AS
select count(fs.id), dc.date_actual, fs.modality_id, fs.facility_id, fs.radiologist_technician_id, fs.referring_practitioner_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_id
where not fs.deleted
group by dc.date_dim_id, fs.modality_id, fs.facility_id, fs.radiologist_technician_id, fs.referring_practitioner_id;