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
    ,enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
) }}

with encounter_combined as(
  select 
    encounter_id
    ,min(claim_start_date) as encounter_start_date
    ,max(claim_end_date) as encounter_end_date
    ,min(admission_date) as admission_date
    ,max(discharge_date) as discharge_date
    ,max(paid_date) as paid_date
    ,sum(paid_amount) as paid_amount
    ,sum(allowed_amount) as allowed_amount
    ,sum(charge_amount) as charge_amount
  from {{ ref('claims_preprocessing__encounter_claim_line_stage')}} mc
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
    ,mc.rendering_npi
    ,mc.billing_npi
    ,mc.facility_npi
    ,mc.facility_name
    ,mc.ms_drg_code
    ,mc.ms_drg_description
    ,data_source
    ,row_number() over (partition by mc.encounter_id order by mc.claim_line_number, mc.claim_start_date) as row_sequence_first
    ,row_number() over (partition by mc.encounter_id order by mc.claim_line_number, mc.claim_end_date) as row_sequence_last
  from {{ ref('claims_preprocessing__encounter_claim_line_stage')}} mc
  where claim_type in ('institutional')
)

select distinct
    {{ cast_string_or_varchar('s.encounter_id') }} as encounter_id
    ,{{ cast_string_or_varchar('s.patient_id') }} as patient_id
    ,{{ cast_string_or_varchar('s.encounter_type') }} as encounter_type
    ,cast(c.encounter_start_date as date) as encounter_start_date
    ,cast(c.encounter_end_date as date) as encounter_end_date
    ,cast(c.admission_date as date) as admission_date
    ,cast(c.discharge_date as date) as discharge_date
    ,cast(first_value(s.admit_source_code) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_source_code
    ,cast(first_value(s.admit_source_description) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_source_description
    ,cast(first_value(s.admit_type_code) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_type_code
    ,cast(first_value(s.admit_type_description) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as admit_type_description
    ,cast(last_value(s.discharge_disposition_code) over(partition by s.encounter_id order by s.row_sequence_last rows between unbounded preceding and unbounded following) as varchar) as discharge_disposition_code
    ,cast(last_value(s.discharge_disposition_description) over(partition by s.encounter_id order by s.row_sequence_last rows between unbounded preceding and unbounded following) as varchar) as discharge_disposition_description
    ,cast(first_value(s.rendering_npi) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as rendering_npi
    ,cast(first_value(s.billing_npi) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as billing_npi
    ,cast(first_value(s.facility_npi) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as facility_npi
    ,cast(first_value(s.facility_name) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as facility_name
    ,cast(first_value(s.ms_drg_code) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as ms_drg_code
    ,cast(first_value(s.ms_drg_description) over(partition by s.encounter_id order by s.row_sequence_first rows between unbounded preceding and unbounded following) as varchar) as ms_drg_description
    ,cast(c.paid_date as date) as paid_date
    ,cast(c.paid_amount as numeric(38,2)) as paid_amount
    ,cast(c.allowed_amount as numeric(38,2)) as allowed_amount
    ,cast(c.charge_amount as numeric(38,2)) as charge_amount
    ,{{ cast_string_or_varchar('s.data_source') }} as data_source
from encounter_stage s
inner join encounter_combined c
	on s.encounter_id = c.encounter_id