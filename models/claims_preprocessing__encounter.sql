

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with table_without_descriptions as (
select
  encounter_id,
  max(patient_id) as patient_id,
  max(encounter_type) as encounter_type,
  max(encounter_start_date) as encounter_start_date,
  max(encounter_end_date) as encounter_end_date,
  max(admission_date) as admission_date,
  min(discharge_date) as discharge_date,
  max(encounter_admit_source_code) as admit_source_code,
  max(encounter_admit_type_code) as admit_type_code,
  max(encounter_discharge_disposition_code) as discharge_disposition_code,
  max(rendering_npi) as rendering_npi,
  max(billing_npi) as billing_npi,
  max(facility_npi) as facility_npi,
  null as facility_name,
  max(ms_drg_code) as ms_drg_code,
  max(paid_date) as paid_date,
  sum(paid_amount) as paid_amount,
  sum(allowed_amount) as allowed_amount,
  sum(charge_amount) as charge_amount,
  max(data_source) as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
group by encounter_id
),


add_descriptions as (
select
  aa.encounter_id,
  aa.patient_id as patient_id,
  aa.encounter_type as encounter_type,
  aa.encounter_start_date as encounter_start_date,
  aa.encounter_end_date as encounter_end_date,
  aa.admit_source_code as admit_source_code,
  bb.admit_source_description,
  aa.admit_type_code as admit_type_code,
  cc.admit_type_description,
  aa.discharge_disposition_code as discharge_disposition_code,
  dd.discharge_disposition_description as
     discharge_disposition_description,
  aa.rendering_npi as rendering_npi,
  aa.billing_npi as billing_npi,
  aa.facility_npi as facility_npi,
  aa.ms_drg_code as ms_drg_code,
  ee.ms_drg_description,
  aa.paid_date as paid_date,
  aa.paid_amount as paid_amount,
  aa.allowed_amount as allowed_amount,
  aa.charge_amount as charge_amount,
  aa.data_source as data_source

from table_without_descriptions aa
     left join {{ ref('terminology__admit_source') }} bb
     on aa.admit_source_code = bb.admit_source_code
     left join {{ ref('terminology__admit_type') }} cc
     on aa.admit_type_code = cc.admit_type_code
     left join {{ ref('terminology__discharge_disposition') }} dd
     on aa.discharge_disposition_code = dd.discharge_disposition_code
     left join {{ ref('terminology__ms_drg') }} ee
     on aa.ms_drg_code = ee.ms_drg_code
     
)


select *
from add_descriptions
