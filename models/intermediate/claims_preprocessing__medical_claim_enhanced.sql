

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with add_encounter_fields as (
select
  aa.*,
  bb.encounter_id as encounter_id,
  bb.encounter_type as encounter_type,
  bb.encounter_type_detail as encounter_type_detail,
  bb.orphan_claim_flag as orphan_claim_flag
from {{ ref('claims_preprocessing__medical_claim') }} aa
     left join {{ ref('claims_preprocessing__encounter_id') }} bb
     on aa.patient_id = bb.patient_id
     and aa.claim_id = bb.claim_id
),


encounter_start_and_end_dates as (
select
  encounter_id,
  min(claim_start_date) as encounter_start_date,
  max(claim_end_date) as encounter_end_date
from add_encounter_fields
group by encounter_id
),

add_encounter_start_and_end_dates as (
select
  aa.*,
  bb.encounter_start_date,
  bb.encounter_end_date
from add_encounter_fields aa
     left join encounter_start_and_end_dates bb
     on aa.encounter_id = bb.encounter_id
),


admit_codes as (
select
  encounter_id,
  max(admit_source_code) as encounter_admit_source_code,
  max(admit_type_code) as encounter_admit_type_code
from add_encounter_start_and_end_dates
where claim_start_date = encounter_start_date
group by encounter_id
),


discharge_code as (
select
  encounter_id,
  max(discharge_disposition_code) as encounter_discharge_disposition_code
from add_encounter_start_and_end_dates
where claim_end_date = encounter_end_date
group by encounter_id
),


add_admit_and_discharge_codes as (
select
  aa.*,
  bb.encounter_admit_source_code,
  bb.encounter_admit_type_code,
  cc.encounter_discharge_disposition_code
from add_encounter_start_and_end_dates aa
     left join admit_codes bb on aa.encounter_id = bb.encounter_id
     left join discharge_code cc on aa.encounter_id = cc.encounter_id
)


select *
from add_admit_and_discharge_codes
