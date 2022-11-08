-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate encounter level details with insitutional data elements taking priority.
-------------------------------------------------------------------------------
-- Modification History
-- TXV 07/2022  Resolving bug that was omitting inst elemts (discharge disp, admit type)
--              due to prof claim link and logic.
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim","core"]
) }}

select distinct
  coalesce(i.encounter_id, p.encounter_id) as encounter_id
  ,coalesce(i.patient_id, p.patient_id) as patient_id
  ,coalesce(i.encounter_type, p.encounter_type) as encounter_type
  ,coalesce(i.encounter_start_date, p.encounter_start_date) as encounter_start_date
  ,coalesce(i.encounter_end_date, p.encounter_end_date) as encounter_end_date
  ,coalesce(i.admission_date, p.admission_date) as admission_date
  ,coalesce(i.discharge_date, p.discharge_date) as discharge_date
  ,coalesce(i.admit_source_code, p.admit_source_code) as admit_source_code
  ,coalesce(i.admit_source_description, p.admit_source_description) as admit_source_description
  ,coalesce(i.admit_type_code, p.admit_type_code) as admit_type_code
  ,coalesce(i.admit_type_description, p.admit_type_description) as admit_type_description
  ,coalesce(i.discharge_disposition_code, p.discharge_disposition_code) as discharge_disposition_code
  ,coalesce(i.discharge_disposition_description, p.discharge_disposition_description) as discharge_disposition_description
  ,coalesce(i.rendering_npi, p.rendering_npi) as rendering_npi
  ,coalesce(i.billing_npi, p.billing_npi) as billing_npi
  ,coalesce(i.facility_npi, p.facility_npi) as facility_npi
  ,coalesce(i.facility_name, p.facility_name) as facility_name
  ,coalesce(i.ms_drg_code, p.ms_drg_code) as ms_drg_code
  ,coalesce(i.ms_drg_description, p.ms_drg_description) as ms_drg_description
  ,coalesce(i.paid_date, p.paid_date) as paid_date
  ,coalesce(i.paid_amount, p.paid_amount) as paid_amount
  ,coalesce(i.allowed_amount, p.allowed_amount) as allowed_amount
  ,coalesce(i.charge_amount, p.charge_amount) as charge_amount
  ,coalesce(i.data_source, p.data_source) as data_source
from {{ ref('encounter_inst_stage')}} i
full outer join {{ ref('encounter_prof_stage')}} p
  on i.encounter_id = p.encounter_id