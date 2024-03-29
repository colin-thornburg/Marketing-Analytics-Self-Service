{% macro check_change_tracking(table_name) %}
  {# Check if the table exists #}
  {% set result = run_query("show tables like '" ~ table_name.name ~ "'") %}
  {% set alter_table = "ALTER TABLE " ~ table_name ~ " SET CHANGE_TRACKING = TRUE" %}
  
  {# If the table exists, check the value of the 'change_tracking' column #}
  {% if result %}
    {% set row = result.columns['change_tracking'].values()[0] %}
    {% do log("Table exists, change_tracking value: " ~ row, info=True) %}
    
    {# If the value is not 'ON', execute the ALTER TABLE statement #}
    {% if row != 'ON' %}
      {% set execute_alter = run_query(alter_table) %}
      {% do log("Executing: " ~ alter_table, info=True) %}
    {% endif %}
  {% else %}
    {# If the table does not exist, execute the ALTER TABLE statement #}
    {% set execute_alter = run_query(alter_table) %}
    {% do log("Table does not exist. Executing: " ~ alter_table, info=True) %}
  {% endif %}
{% endmacro %}