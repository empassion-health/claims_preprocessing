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

select 
    cast(patient_id as varchar) as patient_id
    ,date(min(cast(year as varchar) ||'-'|| cast(month as varchar) || '-01')) as coverage_start_date
    ,date(max(cast(year as varchar) ||'-'|| cast(month as varchar) || '-01')) as coverage_end_date
    ,cast(payer as varchar) as payer
    ,cast(payer_type as varchar) as payer_type
    ,cast('{{ var('source_name')}}' as varchar) as data_source
from {{ var('eligibility')}}
group by 
    patient_id
    ,payer
    ,payer_type