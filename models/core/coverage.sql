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
    cast(patient_id as varchar) as patient_id
    ,enrollment_start_date as coverage_start_date
    ,enrollment_end_date as coverage_end_date
    ,cast(payer as varchar) as payer
    ,cast(payer_type as varchar) as payer_type
    ,cast('{{ var('source_name')}}' as varchar) as data_source
from {{ var('eligibility')}}
