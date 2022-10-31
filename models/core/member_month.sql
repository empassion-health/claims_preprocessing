-------------------------------------------------------------------------------
-- Author       Thu Xuan Vu
-- Created      June 2022
-- Purpose      Provide member enrollment at a month/year grain.
-------------------------------------------------------------------------------
-- Modification History
--
-------------------------------------------------------------------------------
{{ config(
    tags=["eligibility","core"]
    ,enabled=False
) }}

select
	cast(patient_id as varchar) as patient_id
    ,cast(payer as varchar) as payer
    ,cast(month as int) as month
    ,cast(year as int) as year
    ,cast(dual_status as varchar) as dual_status
    ,cast(medicare_status as varchar) as medicare_status
    ,cast('{{ var('source_name')}}' as varchar) as data_source
from {{ var('eligibility')}}