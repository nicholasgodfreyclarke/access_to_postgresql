# access_to_postgresql

Implements a converter class which programmatically converts a ms access database into a postgresql schema.

Works by obtaining information from the pyodbc cursor (table names, data types, etc) 
and generating SQL statements to be executed by psycopg2.

Basic usage:
```python
    >>> converter = Converter(access_con_string, pg_con_string, print_SQL)
    >>> converter.create_schema()
    >>> converter.create_tables()
    >>> converter.insert_data()
    ...
```
  
Requirements:

* pydobc
* psycopg2

# Limitations

As yet does not convert primary keys, indexes, foreign keys.
