-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      List of patients and demographics.
-- Notes        Need row number to select most recent demographic information for a patient.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["eligibility","core"]
) }}

with patient_stage as(
    select
        patient_id
        ,gender
        ,birth_date
        ,race
        ,zip_code
        ,state
        ,deceased_flag
        ,death_date
        ,row_number() over (partition by patient_id order by year DESC) as row_sequence
    from {{ var('eligibility')}}
)

select
    cast(patient_id as varchar) as patient_id
    ,cast(gender as varchar) as gender
    ,cast(birth_date as date) as birth_date
    ,cast(race as varchar) as race
    ,cast(zip_code as varchar) as zip_code
    ,cast(state as varchar) as state
    ,cast(deceased_flag as int) as deceased_flag
    ,cast(death_date as date) as death_date
    ,cast('{{ var('source_name')}}' as varchar) as data_source
from patient_stage
where row_sequence = 1