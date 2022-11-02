-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Crosswalk professional claims to existing institutional encounter.  Matches occur for the same patient, the same encounter type, and overlapping dates.
-- Notes        Ambigous matches are when a prof claim links to more than one inst encounter.  These claims are omitted.
--              An example:  An ED prof claim links to two ED inst encounters. The end of one inst encounter is the start of another but they were not merged due to different
--                  facility NPIs.  Since no other data can help determine which inst encounter the prof claim belongs to, it is not crosswalked.
-------------------------------------------------------------------------------
-- Modification History
-- 09/28/2022 Thu Xuan Vu
--      Changed references of merge_claim_id to claim_id
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}

with prof_inst_encounter_crosswalk as(
  select 
      f.encounter_id
      ,f.patient_id
      ,f.encounter_type
      ,f.encounter_start_date
      ,f.encounter_end_date
      ,d.claim_id
      ,d.claim_start_date
      ,d.claim_end_date
      ,d.encounter_type
      ,case
          when f.encounter_type = 'acute inpatient' and d.encounter_type = 'acute inpatient'
              then 1
          when f.encounter_type = 'hospice ' and d.encounter_type = 'hospice'
              then 1
          when f.encounter_type = 'home health' and d.encounter_type = 'home health'
              then 1
          when f.encounter_type = 'skilled nursing facility' and d.encounter_type = 'skilled nursing facility'
              then 1
          when f.encounter_type = 'ambulatory surgical center' and d.encounter_type = 'ambulatory surgical center'
              then 1
          when f.encounter_type = 'dialysis center' and d.encounter_type = 'dialysis center'
              then 1
          when f.encounter_type = 'mental health center' and d.encounter_type = 'mental health center'
              then 1
          when f.encounter_type = 'emergency department' and d.encounter_type = 'emergency department'
              then 1
          when f.encounter_type = 'outpatient rehabilitation' and d.encounter_type = 'outpatient rehabilitation'
              then 1
          when f.encounter_type = 'outpatient' and d.encounter_type = 'outpatient'
              then 1
                  else 0
       end as link_flag
  from {{ ref('inst_encounter_final')}} f
  inner join {{ ref('encounter_type_union')}} d
      on d.patient_id = f.patient_id
      and d.claim_start_date >= f.encounter_start_date
      and d.claim_start_date <= f.encounter_end_date
  where d.claim_type in ('p','dme','professional')

  )
, ambigous_match as(
  select 
  	claim_id
  from prof_inst_encounter_crosswalk
  where link_flag = 1
  group by claim_id
  having count(*) > 1
)

select 
	encounter_id
    ,c.claim_id
    ,c.patient_id
from prof_inst_encounter_crosswalk c
left join ambigous_match a
	on c.claim_id = a.claim_id
where a.claim_id is null
and c.link_flag = 1
