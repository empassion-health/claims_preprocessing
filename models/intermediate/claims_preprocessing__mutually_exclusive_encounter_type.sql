

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




with encounter_ranking as (
select
  claim_id,
  encounter_type,
  encounter_type_detail,
  case
    when encounter_type = 'acute inpatient' then 1
    when encounter_type = 'skilled nursing' then 2
    when encounter_type = 'emergency department' then 3
    when encounter_type = 'urgent care' then 4
    when encounter_type = 'inpatient rehabilitation' then 5
    when encounter_type = 'inpatient psychiatric' then 6
    when encounter_type = 'inpatient substance abuse' then 7
    when encounter_type = 'ambulatory surgery' then 8
    when encounter_type = 'dialysis' then 9
    when encounter_type = 'hospice' then 10
    when encounter_type = 'home health' then 11
    when encounter_type = 'outpatient rehabilitation' then 12
    when encounter_type = 'outpatient mental health' then 13
    when encounter_type = 'office visit' then 14
    when encounter_type = 'telehealth' then 15
    when encounter_type = 'unmapped' then 16
  end as encounter_rank
  
from {{ ref('claims_preprocessing__encounter_type') }}
  
),


encounter_hierarchy as (
select
  claim_id,
  encounter_type,
  encounter_type_detail
from (
    select
        claim_id,
        encounter_type,
	encounter_type_detail,
	encounter_rank,
        row_number() over (
            partition by claim_id order by encounter_rank
        ) as row_num
    from encounter_ranking
)
where row_num = 1
)


select *
from encounter_hierarchy
