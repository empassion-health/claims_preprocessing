---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Compare institutional claims chronogically to evaluate relationship.  Only continuous 
--                stay encounter types are evaulated for merging.
-- Notes        Claims for a patient are merged if:
--                - dates overlap and the facility is the same
--                - date is off by no more than 1 day, facility is the same, and previous claim has a 
--                  discharge status of 30 - still a patient
---------------------------------------------------------------------------------------------------------
-- Modification History
-- 09/27/2022 Thu Xuan Vu
--      Removed an excess case statement from the strict overlap logic. 
--        previous_claim_start_date >= claim_start_date listed twice
--      Changed references of merge_claim_id to claim_id
---------------------------------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

with previous_claim as(
  select distinct 
    patient_id
    ,lag(claim_id) over (partition by patient_id, encounter_type order by row_sequence) as previous_claim_id
    ,claim_id
    ,encounter_type
    ,lag(claim_start_date) over (partition by patient_id, encounter_type order by row_sequence) as previous_claim_start_date
    ,lag(claim_end_date) over (partition by patient_id, encounter_type order by row_sequence) as previous_claim_end_date
    ,claim_start_date
    ,claim_end_date
    ,lag(discharge_disposition_code) over (partition by patient_id, encounter_type order by row_sequence) as previous_discharge_disposition_code
    ,discharge_disposition_code
    ,lag(facility_npi) over (partition by patient_id, encounter_type order by row_sequence) as previous_facility_npi
    ,facility_npi
    ,row_sequence
  from {{ ref('claims_preprocessing__encounter_type_union')}}
  where claim_type = 'institutional'
  and encounter_type in ('hospice','acute inpatient','skilled nursing facility','home health')
)
,merge_criteria as(
  select 
	case
    	when previous_claim_start_date >= claim_start_date 
  			and previous_claim_start_date <= claim_end_date 
  			and previous_claim_end_date >= claim_start_date
  			and previous_claim_end_date <= claim_end_date 
  			and previous_facility_npi = facility_npi
            	then 'strict overlap'
        when previous_claim_start_date >= claim_start_date 
  			and previous_claim_start_date <= claim_end_date 
  			and previous_facility_npi = facility_npi
        		then 'start overlap'
        when previous_claim_end_date >= claim_start_date 
  			and previous_claim_end_date <= claim_end_date 
  			and previous_facility_npi = facility_npi
        		then 'end overlap'
        when datediff(day,previous_claim_end_date,claim_start_date) = 1 
  			and previous_facility_npi = facility_npi 
  			and previous_discharge_disposition_code = '30'
        	then 'adjacent'
       end as merge_criteria
    ,*
  from previous_claim
)

select 
	patient_id
    ,merge_criteria
    ,previous_claim_id as claim_id_a
    ,claim_id as claim_id_b
    ,encounter_type
    ,previous_claim_start_date
    ,previous_claim_end_date
    ,claim_start_date
    ,claim_end_date
    ,previous_facility_npi
    ,facility_npi
    ,previous_discharge_disposition_code
    ,discharge_disposition_code
    ,row_sequence
from merge_criteria
where merge_criteria is not null