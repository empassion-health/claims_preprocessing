-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate patient coverage details.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}

select distinct
    {{ cast_string_or_varchar('elig.patient_id') }} as patient_id
    , {{ cast_string_or_varchar('elig.member_id') }} as member_id
    , date(elig.enrollment_start_date) as enrollment_start_date
    , date(elig.enrollment_end_date) as enrollment_end_date
    , {{ cast_string_or_varchar('elig.payer') }} as payer
    , {{ cast_string_or_varchar('elig.payer_type') }} as payer_type
    , {{ cast_string_or_varchar('elig.dual_status_code') }} as dual_status_code
    , {{ cast_string_or_varchar('dual.dual_status_description') }} as dual_status_description
    , {{ cast_string_or_varchar('elig.medicare_status_code') }} as medicare_status_code
    , {{ cast_string_or_varchar('status.medicare_status_description') }} as medicare_status_description
    , {{ cast_string_or_varchar('elig.data_source') }} as data_source
from {{ var('eligibility')}} elig
left join {{ ref('terminology__medicare_dual_eligibility')}} dual
    on elig.dual_status_code = dual.dual_status_code
left join {{ ref('terminology__medicare_status')}} status
    on elig.medicare_status_code = status.medicare_status_code
