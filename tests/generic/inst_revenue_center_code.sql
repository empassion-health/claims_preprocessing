{% test inst_revenue_center_code(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/**  Returns rows if institutional claims have a null revenue center code  **/

select {{ column_name }}
from {{ model }}
where claim_type = 'institutional'
and nullif(revenue_center_code,'') is null

{% endtest %}