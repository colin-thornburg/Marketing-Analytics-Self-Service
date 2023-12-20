{#
    Run the following dbt command to create a stream in Snowflake:
        dbt run-operation create_stream --args '{stream_name: "my_macro_stream", table_name: "customer_data_raw", append_only: true, database_name: "colint_dev", schema_name: "raw"}'
#}
{% macro create_stream(stream_name, table_name, append_only=true, database_name=None, schema_name=None) %}
    {% set database_name = database_name if database_name is not none else target.database %}
    {% set schema_name = schema_name if schema_name is not none else target.schema %}
    
    {% set full_stream_name = database_name ~ '.' ~ schema_name ~ '.' ~ stream_name %}
    {% set full_table_name = database_name ~ '.' ~ schema_name ~ '.' ~ table_name %}
    {% set query = 'CREATE OR REPLACE STREAM ' ~ full_stream_name ~ ' ON TABLE ' ~ full_table_name %}
    
    {% if append_only %}
        {% set query = query ~ ' APPEND_ONLY=TRUE' %}
    {% endif %}
    {% set query = query ~ ';' %}
    
    {% if execute %}            
        {% do run_query(query) %}        
    {% endif %}
{% endmacro %}


