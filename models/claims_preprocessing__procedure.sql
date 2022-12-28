

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with unpivot_cte as (

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_1 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_1 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_1 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_2 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_2 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_2 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_3 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_3 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_3 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_4 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_4 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_4 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_5 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_5 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_5 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_6 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_6 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_6 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_7 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_7 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_7 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_8 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_8 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_8 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_9 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_9 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_9 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_10 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_10 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_10 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_11 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_11 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_11 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_12 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_12 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_12 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_13 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_13 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_13 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_14 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_14 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_14 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_15 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_15 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_15 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_16 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_16 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_16 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_17 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_17 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_17 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_18 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_18 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_18 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_19 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_19 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_19 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_20 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_20 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_20 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_21 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_21 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_21 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_22 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_22 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_22 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_23 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_23 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_23 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_24 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_24 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_24 = bb.icd_10_pcs

union

select
  aa.encounter_id as encounter_id,
  aa.patient_id as patient_id,
  aa.procedure_date_25 as procedure_date,
  aa.procedure_code_type as code_type,
  aa.procedure_code_25 as code,
  bb.short_description as description,
  aa.billing_npi as practitioner_npi,
  aa.data_source as data_source
from {{ ref('claims_preprocessing__medical_claim_enhanced') }} aa
     left join {{ ref('terminology__icd_10_pcs') }} bb
     on aa.procedure_code_25 = bb.icd_10_pcs

)


select *
from unpivot_cte
where code is not null
