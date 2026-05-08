{% macro generate_schema_name(custom_schema_name, node) -%}

    {#-
      Override dbt's default schema naming behaviour.

      Default behaviour: {target_schema}_{custom_schema}
        e.g.  BRONZE_BRONZE, BRONZE_SILVER, BRONZE_GOLD  ← messy

      This override: use custom_schema directly when set, else fall back to target schema.
        e.g.  BRONZE, SILVER, GOLD  ← clean

      Reference: https://docs.getdbt.com/docs/build/custom-schemas
    -#}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}

{%- endmacro %}
