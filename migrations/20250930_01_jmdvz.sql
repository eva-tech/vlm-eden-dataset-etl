-- 
-- depends: 20250103_01_AhiOW

CREATE VIEW studies_uploaded_by_date_with_deleted AS
select count(fs.id), dc.date_actual, fs.modality_id, fs.facility_id, fs.radiologist_technician_id, fs.referring_practitioner_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_id
group by dc.date_dim_id, fs.modality_id, fs.facility_id, fs.radiologist_technician_id, fs.referring_practitioner_id;