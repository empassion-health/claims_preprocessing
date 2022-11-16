-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      For merged claims only, determine encounter level data elements.
-- Notes        
-------------------------------------------------------------------------------
-- Modification History
-- 09/28/2022 Thu Xuan Vu
--      Changed references of merge_claim_id to claim_id
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

select 
  mc.group_claim_id as encounter_id
  ,d.patient_id as patient_id
  ,d.encounter_type as encounter_type
  ,min(d.claim_start_date) as encounter_start_date
  ,max(d.claim_end_date) as encounter_end_date
  ,sum(paid_amount) as paid_amount
  ,sum(charge_amount) as charge_amount
from {{ ref('claims_preprocessing__inst_merge_crosswalk')}} mc
inner join {{ ref('claims_preprocessing__encounter_type_union')}} d
  on mc.claim_id = d.claim_id
group by 
  mc.group_claim_id
  ,d.patient_id
  ,d.encounter_type

