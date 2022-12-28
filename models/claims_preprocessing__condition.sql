

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with unpivot_cte as (

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_1 as code,
  bb.short_description as description,
  1 as diagnosis_rank,
  aa.diagnosis_poa_1 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_1 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_1 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_2 as code,
  bb.short_description as description,
  2 as diagnosis_rank,
  aa.diagnosis_poa_2 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_2 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_2 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_3 as code,
  bb.short_description as description,
  3 as diagnosis_rank,
  aa.diagnosis_poa_3 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_3 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_3 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_4 as code,
  bb.short_description as description,
  4 as diagnosis_rank,
  aa.diagnosis_poa_4 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_4 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_4 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_5 as code,
  bb.short_description as description,
  5 as diagnosis_rank,
  aa.diagnosis_poa_5 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_5 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_5 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_6 as code,
  bb.short_description as description,
  6 as diagnosis_rank,
  aa.diagnosis_poa_6 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_6 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_6 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_7 as code,
  bb.short_description as description,
  7 as diagnosis_rank,
  aa.diagnosis_poa_7 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_7 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_7 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_8 as code,
  bb.short_description as description,
  8 as diagnosis_rank,
  aa.diagnosis_poa_8 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_8 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_8 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_9 as code,
  bb.short_description as description,
  9 as diagnosis_rank,
  aa.diagnosis_poa_9 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_9 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_9 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_10 as code,
  bb.short_description as description,
  10 as diagnosis_rank,
  aa.diagnosis_poa_10 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_10 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_10 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_11 as code,
  bb.short_description as description,
  11 as diagnosis_rank,
  aa.diagnosis_poa_11 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_11 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_11 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_12 as code,
  bb.short_description as description,
  12 as diagnosis_rank,
  aa.diagnosis_poa_12 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_12 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_12 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_13 as code,
  bb.short_description as description,
  13 as diagnosis_rank,
  aa.diagnosis_poa_13 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_13 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_13 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_14 as code,
  bb.short_description as description,
  14 as diagnosis_rank,
  aa.diagnosis_poa_14 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_14 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_14 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_15 as code,
  bb.short_description as description,
  15 as diagnosis_rank,
  aa.diagnosis_poa_15 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_15 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_15 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_16 as code,
  bb.short_description as description,
  16 as diagnosis_rank,
  aa.diagnosis_poa_16 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_16 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_16 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_17 as code,
  bb.short_description as description,
  17 as diagnosis_rank,
  aa.diagnosis_poa_17 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_17 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_17 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_18 as code,
  bb.short_description as description,
  18 as diagnosis_rank,
  aa.diagnosis_poa_18 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_18 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_19 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_19 as code,
  bb.short_description as description,
  19 as diagnosis_rank,
  aa.diagnosis_poa_19 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_19 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_19 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_20 as code,
  bb.short_description as description,
  20 as diagnosis_rank,
  aa.diagnosis_poa_20 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_20 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_20 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_21 as code,
  bb.short_description as description,
  21 as diagnosis_rank,
  aa.diagnosis_poa_21 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_21 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_21 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_22 as code,
  bb.short_description as description,
  22 as diagnosis_rank,
  aa.diagnosis_poa_22 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_22 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_22 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_23 as code,
  bb.short_description as description,
  23 as diagnosis_rank,
  aa.diagnosis_poa_23 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_23 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_23 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_24 as code,
  bb.short_description as description,
  24 as diagnosis_rank,
  aa.diagnosis_poa_24 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_24 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_24 = cc.present_on_admit_code

union

select
  aa.encounter_id,
  aa.patient_id,
  aa.claim_start_date as condition_date,
  'discharge_diagnosis' as condition_type,
  aa.diagnosis_code_type as code_type,
  aa.diagnosis_code_25 as code,
  bb.short_description as description,
  25 as diagnosis_rank,
  aa.diagnosis_poa_25 as present_on_admit_code,
  cc.present_on_admit_description as present_on_admit_description,
  aa.data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_cm') }} bb
     on aa.diagnosis_code_25 = bb.icd_10_cm
     left join {{ ref('terminology__present_on_admission') }} cc
     on aa.diagnosis_poa_25 = cc.present_on_admit_code

)



select *
from unpivot_cte
where code is not null
