{# ===========================================================================
   Override: use a model's configured schema as written.

   dbt ships a built-in macro called generate_schema_name that decides which
   schema each model is built in. Its default behaviour is to CONCATENATE: it
   takes the schema from ~/.dbt/profiles.yml and glues the model's configured
   schema onto the end of it. With a profile pointing at `staging` and a model
   configured `+schema: marts`, the default builds a schema called

       market_data.staging_marts

   which is not a name anyone would choose. The reason dbt does this is
   multi-developer safety -- on a shared warehouse it keeps two people's dev
   builds from colliding. That reasoning does not apply to one laptop and one
   account, so the concatenation is pure cost here.

   Step 7 dodged this by having no +schema: config at all: the profile pointed
   directly at `staging` and _sources.yml named `raw` for the source table.
   That worked while there was one folder of models. There are now two, going
   to two different schemas, so the trap has to be dealt with rather than
   avoided.

   This override is the documented way to do that. Overriding a built-in macro
   in dbt is done by defining a macro of the same name in the project's own
   macros/ folder; the project's version wins.

   Behaviour after this file exists:

     no +schema: config   ->  target.schema, i.e. `staging` from the profile
     +schema: marts       ->  `marts`

   Models under models/staging/ have no +schema:, so they are unaffected and
   keep building exactly where they built before. That is worth verifying
   rather than believing, and it is verified in this step.
   =========================================================================== #}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
