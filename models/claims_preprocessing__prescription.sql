-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      Noveber 2022
-- Purpose      Medication list
-- Notes
-------------------------------------------------------------------------------
-- Modification History
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

select
    {{ cast_string_or_varchar('claim_id') }} as claim_id
    , {{ cast_string_or_varchar('claim_line_number') }} as claim_line_number
    , {{ cast_string_or_varchar('patient_id') }} as patient_id
    , {{ cast_string_or_varchar('member_id') }} as member_id
    , {{ cast_string_or_varchar('prescribing_provider_npi') }} as prescribing_provider_npi
    , {{ cast_string_or_varchar('dispensing_provider_npi') }} as dispensing_provider_npi
    , {{ cast_string_or_varchar('dispensing_date') }} as dispensing_date
    , {{ cast_string_or_varchar('ndc_code') }} as ndc_code
    , {{ cast_string_or_varchar('quantity') }} as quantity
    , {{ cast_string_or_varchar('days_supply') }} as days_supply
    , {{ cast_string_or_varchar('refills') }} as refills
    , {{ cast_string_or_varchar('paid_date') }} as paid_date
    , {{ cast_string_or_varchar('paid_amount') }} as paid_amount
    , {{ cast_string_or_varchar('allowed_amount') }} as allowed_amount
    , {{ cast_string_or_varchar('data_source') }} as data_source
from {{ var('pharmacy_claim')}} m