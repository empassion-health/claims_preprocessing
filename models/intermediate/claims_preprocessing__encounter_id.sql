

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




select
  patient_id,
  claim_id,
  encounter_id,
  encounter_type,
  encounter_type_detail,
  0 as orphan_claim_flag
from {{ ref('claims_preprocessing__generate_institutional_encounter_id') }}

union distinct

select
  patient_id,
  claim_id,
  encounter_id,
  encounter_type,
  encounter_type_detail,
  orphan_claim_flag
from {{ ref('claims_preprocessing__professional_claims_in_institutional_encounters') }}

union distinct

select
  patient_id,
  claim_id,
  encounter_id,
  encounter_type,
  encounter_type_detail,
  0 as orphan_claim_flag
from {{ ref('claims_preprocessing__professional_encounters') }}
