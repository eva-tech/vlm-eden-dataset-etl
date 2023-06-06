-- 
-- depends: 20230510_01_V14Kk

CREATE VIEW studies_referred_by_date AS
select count(fs.id),
       dc.date_actual,
       fs.referring_practitioner_id,
       fs.modality_id,
       fs.facility_id
from fact_studies fs
         join dim_calendar dc on dc.date_dim_id = fs.calendar_id
where not fs.deleted
  and fs.referring_practitioner_id is not null
group by dc.date_actual,
         fs.referring_practitioner_id,
         fs.modality_id,
         fs.facility_id