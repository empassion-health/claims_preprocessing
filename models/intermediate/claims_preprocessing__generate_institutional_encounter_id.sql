

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




-- Here we select all rows from the medical_claim table
-- that correspond to encounter types where the encounter
-- may be composed of more than one claim_id.
-- These encounter types are the ones that may have
-- institutional claims.
--
-- Continuation of the logic started in claims_preprocessing__relevant_encounter_type_claims

with
join_every_row_to_all_later_closes as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.encounter_type,
  aa.encounter_type_detail,
  aa.row_num as row_num,
  bb.row_num as row_num_b
from {{ ref('claims_preprocessing__relevant_encounter_type_claims') }}  aa
     inner join
     {{ ref('claims_preprocessing__relevant_encounter_type_claims') }}  bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_type_detail = bb.encounter_type_detail
     and aa.row_num <= bb.row_num
where bb.close_flag = 1
),


find_min_closing_line_for_every_claim_id as (
select
  patient_id,
  claim_id,
  min(row_num_b) as min_closing_row
from join_every_row_to_all_later_closes
group by patient_id, claim_id
),


add_min_closing_row_to_each_claim_id as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.encounter_type as encounter_type,
  aa.encounter_type_detail as encounter_type_detail,
  aa.claim_start_date as claim_start_date,
  aa.claim_end_date as claim_end_date,
  aa.discharge_disposition_code as discharge_disposition_code,
  aa.facility_npi as facility_npi,
  aa.row_num as row_num,
  aa.close_flag as close_flag,
  bb.min_closing_row as min_closing_row
from {{ ref('claims_preprocessing__relevant_encounter_type_claims') }}  aa
     left join find_min_closing_line_for_every_claim_id bb
     on aa.patient_id = bb.patient_id
     and aa.claim_id = bb.claim_id
),


add_encounter_id as (
select
  aa.patient_id as patient_id,
  aa.claim_id as claim_id,
  aa.encounter_type as encounter_type,
  aa.encounter_type_detail as encounter_type_detail,
  aa.claim_start_date as claim_start_date,
  aa.claim_end_date as claim_end_date,
  aa.discharge_disposition_code as discharge_disposition_code,
  aa.facility_npi as facility_npi,
  aa.row_num as row_num,
  aa.close_flag as close_flag,
  aa.min_closing_row as min_closing_row,
  bb.claim_id as encounter_id
from add_min_closing_row_to_each_claim_id aa
     left join add_min_closing_row_to_each_claim_id bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_type_detail = bb.encounter_type_detail
     and aa.min_closing_row = bb.row_num
)





select
  patient_id,
  claim_id,
  encounter_id,
  encounter_type,
  encounter_type_detail
from add_encounter_id


