<<<<<<< HEAD
select
    {{ cast_string_or_varchar('null') }} as claim_id
    , {{ cast_string_or_varchar('null') }} as claim_line_number
    , {{ cast_string_or_varchar('null') }} as patient_id
    , {{ cast_string_or_varchar('null') }} as member_id
    , {{ cast_string_or_varchar('null') }} as prescribing_provider_npi
    , {{ cast_string_or_varchar('null') }} as dispensing_provider_npi
    , {{ cast_string_or_varchar('null') }} as dispensing_date
    , {{ cast_string_or_varchar('null') }} as ndc_code
    , {{ cast_string_or_varchar('null') }} as quantity
    , {{ cast_string_or_varchar('null') }} as days_supply
    , {{ cast_string_or_varchar('null') }} as refills
    , {{ cast_string_or_varchar('null') }} as paid_date
    , {{ cast_string_or_varchar('null') }} as paid_amount
    , {{ cast_string_or_varchar('null') }} as allowed_amount
    , {{ cast_string_or_varchar('null') }} as data_source
where 1=0
=======
select * from {{source('claims_common','pharmacy_claim')}}

--
-- select
--     cast(null as {{ dbt.type_string() }}) as CLAIM_ID
--     ,cast(null as {{ dbt.type_string() }}) as CLAIM_LINE_NUMBER
--     ,cast(null as {{ dbt.type_string() }}) as PATIENT_ID
--     ,cast(null as {{ dbt.type_string() }}) as MEMBER_ID
--     ,cast(null as {{ dbt.type_string() }}) as PRESCRIBING_PROVIDER_NPI
--     ,cast(null as {{ dbt.type_string() }}) as DISPENSING_PROVIDER_NPI
--     ,cast(null as date) as DISPENSING_DATE
--     ,cast(null as {{ dbt.type_string() }}) as NDC_CODE
--     ,cast(null as int) as QUANTITY
--     ,cast(null as int) as DAYS_SUPPLY
--     ,cast(null as int) as REFILLS
--     ,cast(null as date) as PAID_DATE
--     ,cast(null as numeric) as PAID_AMOUNT
--     ,cast(null as numeric) as ALLOWED_AMOUNT
--     ,cast(null as {{ dbt.type_string() }}) as DATA_SOURCE
-- limit 0
>>>>>>> 332d798ce52b4d8e860659805f1327a4b0076415
