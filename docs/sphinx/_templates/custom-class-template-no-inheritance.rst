{{ name | escape | underline}}

.. currentmodule:: {{ module }}

{%- set callable_class_attrs = ['schema_func', 'extraction_func'] %}

.. autoclass:: {{ objname }}
   :members:
   :special-members: __call__, __add__, __mul__
   :no-inherited-members:

   {% block methods %}
   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
      :nosignatures:
   {% for item in methods %}
      {%- if item not in inherited_members and item not in callable_class_attrs %}
         ~{{ name }}.{{ item }}
      {%- endif -%}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {% for item in methods if item in callable_class_attrs and item not in inherited_members %}
      ~{{ name }}.{{ item }}
   {%- endfor %}
   {% endif %}
   {% endblock %}