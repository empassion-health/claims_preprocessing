

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




-- Here we select all rows from the medical_claim table
-- that correspond to encounter types where the encounter
-- may be composed of more than one claim_id.
-- These encounter types are the ones that may have
-- institutional claims.


with relevant_encounter_type_rows_from_medical_claim as (
select
  aa.*,
  bb.encounter_type as encounter_type,
  bb.encounter_type_detail as encounter_type_detail
from {{ ref('claims_preprocessing__medical_claim') }} aa
     left join {{ ref('claims_preprocessing__mutually_exclusive_encounter_type') }} bb
     on aa.claim_id = bb.claim_id
where bb.encounter_type_detail
		   in ('acute inpatient - institutional',
                       'inpatient rehabilitation - institutional',
                       'inpatient psychiatric - institutional',
                       'inpatient substance abuse - institutional',
                       'skilled nursing - institutional',
                       'ambulatory surgery - institutional',
                       'dialysis - institutional',
                       'emergency department - institutional',
                       'urgent care - institutional',
                       'outpatient mental health - institutional',
                       'hospice - institutional',
                       'outpatient rehabilitation - institutional',
                       'home health - institutional')
),


key_counts as (
select
  claim_id,
  count(distinct patient_id) as patient_id_count,
  count(distinct claim_start_date) as claim_start_date_count,
  count(distinct claim_end_date) as claim_end_date_count,
  count(distinct discharge_disposition_code)
                 as discharge_disposition_code_count,
  count(distinct facility_npi) as facility_npi_count

from relevant_encounter_type_rows_from_medical_claim
group by claim_id
having
  patient_id_count = 1 and
  discharge_disposition_code_count = 1 and
  claim_start_date_count = 1 and
  claim_end_date_count = 1 and
  facility_npi_count = 1
),


relevant_encounter_type_rows_that_meet_counts_requirement as (
select rel_enc_types.*
from relevant_encounter_type_rows_from_medical_claim as rel_enc_types
    left join key_counts
        on rel_enc_types.claim_id = key_counts.claim_id
),


relevant_encounter_type_claims as (
select
  claim_id,
  max(patient_id) as patient_id,
  max(encounter_type) as encounter_type,
  max(encounter_type_detail) as encounter_type_detail,
  min(claim_start_date) as claim_start_date,
  max(claim_end_date) as claim_end_date,
  max(discharge_disposition_code) as discharge_disposition_code,
  max(facility_npi) as facility_npi
from relevant_encounter_type_rows_that_meet_counts_requirement
group by claim_id
),


-- relevant_encounter_type_claims_that_meet_data_quality_requirements as (
-- select *
-- from relevant_encounter_type_claims
-- where claim_start_date <= claim_end_date
-- ),


relevant_encounter_type_claims_with_row_num as (
select
  patient_id,
  claim_id,
  encounter_type,
  encounter_type_detail,
  claim_start_date,
  claim_end_date,
  discharge_disposition_code,
  facility_npi,
  row_number() over (
    partition by patient_id, encounter_type
    order by claim_end_date, claim_start_date, claim_id
  ) as row_num
from relevant_encounter_type_claims
where claim_start_date <= claim_end_date
),

check_for_merges_with_larger_row_num as (
select
  aa.patient_id,
  aa.encounter_type,
  aa.encounter_type_detail,
  aa.claim_id as claim_id_a,
  bb.claim_id as claim_id_b,
  aa.row_num as row_num_a,
  bb.row_num as row_num_b,
  case
    -- Claims have same claim_end_date and should be merged:
    when (aa.claim_end_date = bb.claim_end_date
          and aa.facility_npi = bb.facility_npi) then 1

    -- Claims have different claim_end_date and are
    -- adjacent and should be merged:
    when (aa.claim_end_date + 1 = bb.claim_start_date
          and aa.facility_npi = bb.facility_npi
	  and aa.discharge_disposition_code = '30') then 1

    -- Claims have different claim_end_date and overlap
    -- and should be merged:
    when (aa.claim_end_date >= bb.claim_start_date
          and aa.facility_npi = bb.facility_npi ) then 1
    else 0
  end as merge_flag

from relevant_encounter_type_claims_with_row_num aa
     inner join relevant_encounter_type_claims_with_row_num bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_type_detail = bb.encounter_type_detail
     and aa.row_num < bb.row_num
),


merges_with_larger_row_num as (
select
  patient_id,
  encounter_type,
  encounter_type_detail,
  claim_id_a,
  claim_id_b,
  row_num_a,
  row_num_b,
  merge_flag
from check_for_merges_with_larger_row_num
where merge_flag = 1
),


claim_ids_that_merge_with_a_larger_row_num as (
select distinct claim_id_a as claim_id
from merges_with_larger_row_num
),


claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num as (
select distinct aa.claim_id as claim_id
from relevant_encounter_type_claims_with_row_num aa
     inner join
     merges_with_larger_row_num bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_type_detail = bb.encounter_type_detail
     and bb.row_num_a < aa.row_num
     and bb.row_num_b > aa.row_num
),


relevant_encounter_type_claims_with_row_num_and_close_flag as (
select
  rel.patient_id,
  rel.claim_id,
  rel.encounter_type,
  rel.encounter_type_detail,
  rel.claim_start_date,
  rel.claim_end_date,
  rel.discharge_disposition_code,
  rel.facility_npi,
  rel.row_num ,
  case when rel_w_lar.claim_id is null and rel_w_small.claim_id is null then 1
    else 0
  end as close_flag
from relevant_encounter_type_claims_with_row_num as rel
left join claim_ids_that_merge_with_a_larger_row_num as rel_w_lar
        on rel.claim_id = rel_w_lar.claim_id
left join claim_ids_having_a_smaller_row_num_merging_with_a_larger_row_num as rel_w_small
        on rel.claim_id = rel_w_small.claim_id


)
    select * From relevant_encounter_type_claims_with_row_num_and_close_flag
