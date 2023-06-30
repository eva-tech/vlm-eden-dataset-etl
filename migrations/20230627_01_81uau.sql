-- 
-- depends: 20230606_01_cY4nG

alter table dim_technicians
    add fact_studies_id integer not null;

alter table dim_technicians
    add study_external_id varchar not null;

alter table dim_technicians
    add constraint dim_technicians_fact_studies_id_fk
        foreign key (fact_studies_id) references fact_studies;

alter table dim_technicians
    drop constraint dim_technicians_name_key;

alter table dim_technicians
    alter column created_at set default now();

alter table dim_technicians
    alter column updated_at set default now();
