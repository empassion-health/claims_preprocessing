{% test encounter_type_mapping(model, column_name) %}
{{ config(enabled=var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))) }}
/** Returns rows if encounter type mapping does not exist in seed file
    Used custom test to account for unmapped rows.  Do not want to add them to the seed and test passes if they exist.   **/

select m.{{ column_name }}
from {{ model }} m
left join {{ ref('terminology__encounter_type')}} e
    on e.encounter_type = m.encounter_type
where m.encounter_type <> 'unmapped'
and e.encounter_type is null

{% endtest %}