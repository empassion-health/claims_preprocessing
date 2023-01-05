

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with relevant_rows_from_medical_claim as (
select
  aa.patient_id,
  aa.claim_id,
  aa.claim_id as encounter_id,
  bb.encounter_type,
  bb.encounter_type_detail
from {{ ref('claims_preprocessing__medical_claim') }} aa
     left join {{ ref('claims_preprocessing__mutually_exclusive_encounter_type') }} bb
     on aa.claim_id = bb.claim_id
where bb.encounter_type_detail in ('office visit','telehealth','unmapped')
)


select distinct *
from relevant_rows_from_medical_claim

