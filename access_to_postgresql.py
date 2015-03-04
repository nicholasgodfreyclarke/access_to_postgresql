import os
import json
import pyodbc
import psycopg2
import time
import sys

# Refer to https://code.google.com/p/pyodbc/wiki/Cursor for information on
# cursor.tables and cursor.columns field names


class Converter:

    def __init__(self, access_con_string, pg_con_string, print_SQL):

        self.access_cur = pyodbc.connect(access_con_string).cursor()

        self.pg_con = psycopg2.connect(pg_con_string)
        self.pg_cur = self.pg_con.cursor()

        self.print_SQL = print_SQL

        self.schema_name = self.get_access_db_name()

    def get_access_db_name(self):

        # The full path of the database is stored in the table information
        # We can parse it to get the file name (to use as scheme_name)
        for table in self.access_cur.tables():
            return os.path.splitext(os.path.basename(table.table_cat))[0]

    def create_schema(self):

        SQL = """
        CREATE SCHEMA "{schema_name}"
        """.format(schema_name=self.schema_name)

        if self.print_SQL:
            print SQL

        self.pg_cur.execute(SQL)
        self.pg_con.commit()

    def create_tables(self):

        # Generate list of tables in schema
        table_list = list()
        for table in self.access_cur.tables():
            if table.table_type == "TABLE":
                table_list += [table.table_name, ]

        for table in table_list:
            SQL = """
            CREATE TABLE "{schema}"."{table}"
            (
            """.format(schema=self.schema_name, table=table)

            SQL += self.create_fields(table)

            SQL += """
            ) """

            if self.print_SQL:
                print SQL

            self.pg_cur.execute(SQL)
            self.pg_con.commit()

    def create_fields(self, table):

        postgresql_fields = {
            'COUNTER': 'serial',  # autoincrement
            'VARCHAR': 'text',  # text
            'LONGCHAR': 'text',  # memo
            'BYTE': 'integer',  # byte
            'SMALLINT': 'integer',  # integer
            'INTEGER': 'bigint',  # long integer
            'REAL': 'real',  # single
            'DOUBLE': 'double precision',  # double
            'DATETIME': 'timestamp',  # date/time
            'CURRENCY': 'money',  # currency
            'BIT':  'boolean',  # yes/no
        }

        SQL = ""
        field_list = list()
        for column in self.access_cur.columns(table=table):
            if column.type_name in postgresql_fields:
                field_list += ['"' + column.column_name + '"' +
                               " " + postgresql_fields[column.type_name], ]
            elif column.type_name == "DECIMAL":
                field_list += ['"' + column.column_name + '"' +
                               " numeric(" + str(column.column_size) + "," +
                               str(column.decimal_digits) + ")", ]
            else:
                print "column " + table + "." + column.column_name +
                " has uncatered for type: " + column.type_name

        return ",\n ".join(field_list)

    def insert_data(self):

        # Generate list of tables in schema
        table_list = list()
        for table in self.access_cur.tables():
            if table.table_type == "TABLE":
                table_list += [table.table_name, ]

        for table in table_list:
            data = self.get_access_data(table)

            # check that data exists
            if data != []:
                # Create format string (eg (%s,%s,%s)
                # the same size as the number of fields)
                format_string = "(" + ",".join(["%s", ]*len(data[0])) + ")\n"

                # pre-bind the arguments before executing - for speed
                args_string = ','.join(self.pg_cur.mogrify(format_string, x)
                                       for x in data)

                SQL = """INSERT INTO "{schema_name}"."{table_name}"
                VALUES {value_list}""".format(schema_name=self.schema_name,
                                              table_name=table,
                                              value_list=args_string)

                if self.print_SQL:
                    print SQL

                self.pg_cur.execute(SQL)

                self.pg_con.commit()

    def get_access_data(self, table):

        SQL = """SELECT *
        FROM {table_name}""".format(table_name=table)

        self.access_cur.execute(SQL)

        rows = self.access_cur.fetchall()

        data = list()
        for row in rows:
            data += [row, ]

        return data

if __name__ == "__main__":
    if len(sys.argv) != 2
    and os.path.exists(config_path)
    and config_path.endswith('.json'):
        exit("Requires a config json file")

    config_path = os.path.abspath(sys.argv[1])

    config_data = json.load(open(config_path))

    pg_con_string = config_data['postgresql_connection_string']
    print_SQL = config_data['print_SQL']

    for access_con_string in config_data['access_connection_strings']:

        converter = Converter(access_con_string, pg_con_string, print_SQL)

        converter.create_schema()

        converter.create_tables()

        converter.insert_data()
