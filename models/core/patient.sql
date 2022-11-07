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
        ,race
        ,birth_date
        ,death_date
        ,death_flag
        ,first_name
        ,last_name
        ,address
        ,city
        ,state
        ,zip_code
        ,phone
        ,data_source
        ,row_number() over (partition by patient_id order by enrollment_end_date DESC) as row_sequence
    from {{ var('eligibility')}}
)

select
    cast(patient_id as varchar) as patient_id
    ,cast(gender as varchar) as gender
    ,cast(race as varchar) as race
    ,cast(birth_date as date) as birth_date
    ,cast(death_date as date) as death_date
    ,cast(death_flag as int) as death_flag
    ,cast(first_name as varchar) as first_name
    ,cast(last_name as varchar) as last_name
    ,cast(address as varchar) as address
    ,cast(city as varchar) as city
    ,cast(state as varchar) as state
    ,cast(zip_code as varchar) as zip_code
    ,cast(phone as varchar) as phone
    ,cast(data_source as varchar) as data_source
from patient_stage
where row_sequence = 1