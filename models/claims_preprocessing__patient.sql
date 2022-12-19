-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      List of patients and demographics.
-- Notes        Need row number to select most recent demographic information for a patient.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

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
    {{ cast_string_or_varchar('patient_id') }} as patient_id
    ,{{ cast_string_or_varchar('gender') }} as gender
    ,{{ cast_string_or_varchar('race') }} as race
    ,cast(birth_date as date) as birth_date
    ,cast(death_date as date) as death_date
    ,cast(death_flag as int) as death_flag
    ,{{ cast_string_or_varchar('first_name') }} as first_name
    ,{{ cast_string_or_varchar('last_name') }} as last_name
    ,{{ cast_string_or_varchar('address') }} as address
    ,{{ cast_string_or_varchar('city') }} as city
    ,{{ cast_string_or_varchar('state') }} as state
    ,{{ cast_string_or_varchar('zip_code') }} as zip_code
    ,{{ cast_string_or_varchar('phone') }} as phone
    ,{{ cast_string_or_varchar('data_source') }} as data_source
from patient_stage
where row_sequence = 1