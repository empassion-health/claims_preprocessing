

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with relevant_professional_encounter_rows_from_medical_claim as (
select
  aa.claim_id,
  aa.patient_id,
  aa.claim_start_date,
  aa.claim_end_date,
  bb.encounter_type,
  bb.encounter_type_detail
  
from {{ ref('claims_preprocessing__medical_claim') }} aa
     left join {{ ref('claims_preprocessing__mutually_exclusive_encounter_type') }} bb
     on aa.claim_id = bb.claim_id
where bb.encounter_type_detail
		   in ('acute inpatient - professional',
                       'inpatient rehabilitation - professional',
                       'inpatient psychiatric - professional',
                       'inpatient substance abuse - professional',
                       'skilled nursing - professional',
                       'ambulatory surgery - professional',
                       'dialysis - professional',
                       'emergency department - professional',
                       'urgent care - professional',
                       'outpatient mental health - professional',
                       'hospice - professional',
                       'outpatient rehabilitation - professional',
                       'home health - professional') 
),


relevant_professional_encounter_claims as (
select
  claim_id,
  patient_id,
  min(claim_start_date) as claim_start_date,
  max(claim_end_date) as claim_end_date,
  max(encounter_type) as encounter_type,
  max(encounter_type_detail) as encounter_type_detail
from relevant_professional_encounter_rows_from_medical_claim
group by patient_id, claim_id
),


find_orphan_professional_claims as (
select
  aa.patient_id,
  aa.claim_id,
  bb.encounter_id,
  aa.encounter_type,
  aa.encounter_type_detail,
  case
    when bb.encounter_id is null then 1
    else 0
  end as orphan_claim_flag
  
from relevant_professional_encounter_claims aa
     left join
     {{ ref('claims_preprocessing__institutional_encounter_start_and_end_dates') }} bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_type = bb.encounter_type
     and (aa.claim_start_date
          between bb.encounter_start_date and bb.encounter_end_date)
     and (aa.claim_end_date
          between bb.encounter_start_date and bb.encounter_end_date)
	 
)



select *
from find_orphan_professional_claims
