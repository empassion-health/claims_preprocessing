

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with claims_for_institutional_encounters as (
select
  aa.*,
  bb.encounter_type as encounter_type,
  bb.encounter_type_detail as encounter_type_detail,
  bb.encounter_id as encounter_id
from {{ ref('claims_preprocessing__medical_claim') }} aa
     inner join {{ ref('claims_preprocessing__generate_institutional_encounter_id') }} bb
     on aa.patient_id = bb.patient_id
     and aa.claim_id = bb.claim_id
),


institutional_encounter_start_and_end_dates as (
select
  patient_id,
  encounter_id,
  max(encounter_type) as encounter_type,
  max(encounter_type_detail) as encounter_type_detail,
  min(claim_start_date) as encounter_start_date,
  max(claim_end_date) as encounter_end_date
from claims_for_institutional_encounters
group by patient_id, encounter_id
)


select *
from institutional_encounter_start_and_end_dates

