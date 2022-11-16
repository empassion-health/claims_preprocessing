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
    cast(elig.patient_id as varchar) as patient_id
    , cast(elig.member_id as varchar) as member_id
    , date(elig.enrollment_start_date) as enrollment_start_date
    , date(elig.enrollment_end_date) as enrollment_end_date
    , cast(elig.payer as varchar) as payer
    , cast(elig.payer_type as varchar) as payer_type
    , cast(elig.dual_status_code as varchar) as dual_status_code
    , cast(dual.dual_status_description as varchar) as dual_status_description
    , cast(elig.medicare_status_code as varchar) as medicare_status_code
    , cast(status.medicare_status_description as varchar) as medicare_status_description
    , cast(elig.data_source as varchar) as data_source
from {{ var('eligibility')}} elig
left join {{ ref('terminology__medicare_dual_eligibility')}} dual
    on elig.dual_status_code = dual.dual_status_code
left join {{ ref('terminology__medicare_status')}} status
    on elig.medicare_status_code = status.medicare_status_code
