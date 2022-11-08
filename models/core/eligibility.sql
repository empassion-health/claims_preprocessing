-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Populate patient coverage details.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["eligibility","core"]
) }}

select distinct
    cast(elig.patient_id as varchar) as patient_id
    , cast(elig.member_id as varchar) as member_id
    , date(elig.enrollment_start_date) as enrollment_start_date
    , date(elig.enrollment_end_date) as enrollment_end_date
    , cast(elig.payer as varchar) as payer
    , cast(elig.payer_type as varchar) as payer_type
    , cast(elig.dual_status_code as varchar) as dual_status_code
    , cast(dual.description as varchar) as dual_status_description
    , cast(elig.medicare_status_code as varchar) as medicare_status_code
    , cast(status.description as varchar) as medicare_status_description
    , cast(elig.data_source as varchar) as data_source
from {{ var('eligibility')}} elig
left join {{ source('tuva_terminology','medicare_dual_eligibility')}} dual
    on elig.dual_status_code = dual.code
left join {{ source('tuva_terminology','medicare_status_code')}} status
    on elig.medicare_status_code = status.code
