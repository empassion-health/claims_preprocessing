-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate encounter level details.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["medical_claim"]
) }}

with encounter_combined as(
  select 
    encounter_id
    ,min(claim_start_date) as encounter_start_date
    ,max(claim_end_date) as encounter_end_date
    ,sum(cast(paid_amount as numeric(38,2))) as paid_amount
    ,sum(charge_amount) as charge_amount
  from {{ ref('encounter_line_stage')}} mc
  group by
      encounter_id
)
, encounter_stage as(
  select
    mc.encounter_id
    ,mc.patient_id
    ,mc.encounter_type
    ,mc.admit_source_code
    ,mc.admit_source_description
    ,mc.admit_type_code
    ,mc.admit_type_description
    ,mc.discharge_disposition_code
    ,mc.discharge_disposition_description
    ,mc.billing_npi as physician_npi
    ,cast(null as varchar) as location
    ,mc.facility_npi
    ,mc.ms_drg
    ,cast('{{ var('source_name')}}' as varchar) as data_source
    ,row_number() over (partition by mc.encounter_id order by mc.claim_line_number, mc.claim_start_date) as row_sequence_first
    ,row_number() over (partition by mc.encounter_id order by mc.claim_line_number, mc.claim_end_date) as row_sequence_last
  from {{ ref('encounter_line_stage')}} mc
  where claim_type = 'I'
)

select distinct
    cast(s.encounter_id as varchar) as encounter_id
    ,cast(s.patient_id as varchar) as patient_id
    ,cast(s.encounter_type as varchar) as encounter_type
    ,cast(c.encounter_start_date as date) as encounter_start_date
    ,cast(c.encounter_end_date as date) as encounter_end_date
    ,cast(first_value(s.admit_source_code) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_source_code
    ,cast(first_value(s.admit_source_description) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_source_description
    ,cast(first_value(s.admit_type_code) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_type_code
    ,cast(first_value(s.admit_type_description) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_type_description
    ,cast(last_value(s.discharge_disposition_code) over(partition by s.encounter_id order by s.row_sequence_last rows between unbounded preceding and unbounded following) as varchar) as discharge_disposition_code
    ,cast(last_value(s.discharge_disposition_description) over(partition by s.encounter_id order by s.row_sequence_last rows between unbounded preceding and unbounded following) as varchar) as discharge_disposition_description
    ,cast(first_value(s.physician_npi) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as physician_npi
    ,cast(s.location as varchar) as location
    ,cast(first_value(s.facility_npi) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as facility_npi
    ,cast(first_value(s.ms_drg) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as ms_drg
    ,cast(c.paid_amount as numeric(38,2)) as paid_amount
    ,cast(c.charge_amount as numeric(38,2)) as charge_amount
    ,cast(s.data_source as varchar) as data_source
from encounter_stage s
inner join encounter_combined c
	on s.encounter_id = c.encounter_id