---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      For merged claims only, determine encounter level data elements.
-- Notes        Query is the same as inst_encounter_merge except it omits merged claims to create an encounter for non merged institutional claims.
---------------------------------------------------------------------------------------------------------
-- Modification History
-- 09/28/2022 Thu Xuan Vu
--      Changed references of merge_claim_id to claim_id
---------------------------------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}

select
    d.claim_id as encounter_id
    ,d.patient_id as patient_id
    ,d.encounter_type as encounter_type
    ,min(d.claim_start_date) as encounter_start_date
    ,max(d.claim_end_date) as encounter_end_date
    ,sum(paid_amount) as paid_amount
    ,sum(charge_amount) as charge_amount
from {{ ref('encounter_type_mapping')}} d
left join {{ ref('inst_merge_crosswalk')}} c
    on d.claim_id = c.claim_id
where d.claim_type = 'I'
and c.claim_id is null
group by 
  d.claim_id
  ,d.patient_id
  ,d.encounter_type