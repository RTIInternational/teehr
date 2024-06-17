attributes:
    - name (pk)
    - type
    - units (fk)

measurement_units:
    - name (pk)
    - dimension

configurations:
    - name (pk)
    - owner
    - description

variables:
    - name
    - description

