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