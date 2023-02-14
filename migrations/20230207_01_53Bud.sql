-- 
-- depends: 20230104_01_r9HiK

CREATE VIEW studies_uploaded_by_date AS
select count(fs.id), dc.date_actual, fs.modality_id, fs.facility_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_id
group by dc.date_dim_id, fs.modality_id, fs.facility_id;

CREATE VIEW studies_signed_by_date AS
select count(fs.id),
       dc.date_actual,
       fs.signed_by_id,
       fs.modality_id,
       fs.facility_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_sign_at_id

where not fs.deleted
  and fs.signed_by_id is not null
group by dc.date_actual,
         fs.signed_by_id,
         fs.modality_id,
         fs.facility_id
