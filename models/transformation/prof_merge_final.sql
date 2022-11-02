---------------------------------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      May 2022
-- Purpose      Group office visit claims from the same day with the same provider.
-- Notes
---------------------------------------------------------------------------------------------------------
-- Modification History
---------------------------------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}

with same_day_same_provider as(
  select
      map.patient_id
      ,map.encounter_type
      ,map.claim_start_date
      ,map.billing_npi
      ,map.claim_type
      ,min(map.claim_id) as encounter_id
  from {{ ref('encounter_type_union')}} map
  left join {{ ref('prof_inst_encounter_crosswalk')}} xwalk
    on map.claim_id = xwalk.claim_id
  where xwalk.claim_id is null
  and map.encounter_type not in ('inpatient psychiatric','inpatient rehabilitation','acute inpatient','skilled nursing facility','home health','hospice')
  and map.claim_type in ('p','professional')
  group by 
      map.patient_id
      ,map.encounter_type
      ,map.claim_start_date
      ,map.billing_npi
      ,map.claim_type
  having count(*) > 1
)
, same_day_only as(
  select
      map.patient_id
      ,map.encounter_type
      ,map.claim_start_date
      ,map.claim_type
      ,min(map.claim_id) as encounter_id
  from {{ ref('encounter_type_union')}} map
  left join {{ ref('prof_inst_encounter_crosswalk')}} xwalk
    on map.claim_id = xwalk.claim_id
  where xwalk.claim_id is null
  and map.encounter_type in ('inpatient psychiatric','inpatient rehabilitation','acute inpatient','skilled nursing facility','home health','hospice')
  and map.claim_type in ('p','professional')
  group by 
      map.patient_id
      ,map.encounter_type
      ,map.claim_start_date
      ,map.claim_type
  having count(*) > 1
)


select
    map.patient_id
    ,same.encounter_id
    ,map.claim_id
    ,map.encounter_type
    ,map.claim_start_date
    ,map.billing_npi
    ,map.claim_type
from same_day_same_provider same
inner join {{ ref('encounter_type_union')}} map
    on same.patient_id = map.patient_id
    and same.encounter_type = map.encounter_type
    and same.claim_start_date = map.claim_start_date
    and same.billing_npi = map.billing_npi
    and same.claim_type = map.claim_type

union all

select
    map.patient_id
    ,day.encounter_id
    ,map.claim_id
    ,map.encounter_type
    ,map.claim_start_date
    ,map.billing_npi
    ,map.claim_type
from same_day_only day
inner join {{ ref('encounter_type_union')}} map
    on day.patient_id = map.patient_id
    and day.encounter_type = map.encounter_type
    and day.claim_start_date = map.claim_start_date
    and day.claim_type = map.claim_type
