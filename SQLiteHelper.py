import sqlite3
import pydantic
from pydantic import BaseModel
from configparser import ConfigParser


def load_config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config


def parse_table_config(input_config):
    return_list = []
    for key, value in input_config.items():
        return_list.append((f'{key.replace("*", "")} {value}', key[0] == '*'))
    return return_list


class SQLiteHelper():

    def __init__(self, db_file, db_name):
        self.__config__ = load_config(db_file, db_name)
        self.__config_list__ = parse_table_config(self.__config__)
        self.__establish_db_conn__(db_name)
        self.db_name = db_name
        self.__create_table__(db_name, self.__config_list__)

    def __create_table__(self, table_name, table_layout):
        """Create SQLite db if it doesn't exist. """

        __command_string__ = f"""CREATE TABLE IF NOT EXISTS {table_name} ("""
        __primary_keys__ = 'PRIMARY KEY ('

        for line in table_layout:
            __command_string__ += f'{line[0]}, '
            if line[1]:
                __primary_keys__ += f"{line[0].split(' ')[0]}"

        execute_command = f"""{__command_string__} {__primary_keys__}))"""
        try:
            self.cursor.execute(execute_command)

        except sqlite3.OperationalError as se:
            print(f'Exception Occurred: {se}') #

    def __establish_db_conn__(self, db_name):
        """Handle creating the SQLite file/open the existing one."""

        self.conn = sqlite3.connect(f'{db_name}.db')
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def execute_query(self, query, params=None):
        """Function for handling executing SQL queries. params will be None in almost all cases, added for some things
        planned for the future. """
        try:
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            result = self.cursor.fetchall()
            return [dict(row) for row in result]

        except Exception as e:
            self.conn.rollback()
            print(f'Exception occurred, rolled back any changes. Error: {e}')
            return []

    def select_data(self, selection_items, selection_where=None):

        query = f'SELECT {selection_items} FROM {self.db_name}'
        if selection_where:
            query += f' WHERE {selection_where}'
        try:
            data = self.execute_query(query)
            self.conn.commit()
            print(data)
            if data:
                return data
            else:
                return f'No data returned.'

        except Exception as e:
            self.conn.rollback()
            print(f'Select failed, rolled back. Error: {e}')
            return f'Select failed, rolled back. Error: {e}'

    def insert_data(self, query_columns, query_values):
        """Insert data into SQLite Database.
        query_columns takes a tuple of the columns in the table where you want to insert data.
        query_values takes a tuple of the values you would like to insert into the db."""

        columns = ', '.join(query_columns)

        formatted_values = []
        for value in query_values:
            if isinstance(value, str):
                formatted_values.append(f"'{value}'")
            elif value is None:
                formatted_values.append('NULL')
            else:
                formatted_values.append(str(value))

        values = ', '.join(formatted_values)
        query = f'INSERT INTO {self.db_name} ({columns}) VALUES ({values})'
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print("Data inserted successfully.")
            return "Data inserted successfully."

        except Exception as e:
            self.conn.rollback()
            print(f'Insertion failed, rolled back. Error: {e}')
            return f'Insertion failed, rolled back. Error: {e}'

    def delete_data(self, delete_from_column, value_to_delete):
        """Used for deleting data from the table associated with the SQLite file."""

        query = f"DELETE FROM {self.db_name} WHERE {delete_from_column} = '{value_to_delete}'"
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print("Data deleted successfully.")
            return "Data deleted successfully"

        except Exception as e:
            self.conn.rollback()
            print(f'Deletion failed, rolled back. Error: {e}')
            return f'Deletion failed, rolled back. Error: {e}'

    def update_data(self, update_data_dictionaries, where_clause):
        """Update SQLite DB data. Takes a list of dictionaries.
        key of the dictionary is the column that is being updated,
        value is the value to update.
        where_clause takes logic for the WHERE statement. Example: "age=0" for updating column values where age=0 """

        merged_dict = {}
        for dictionary in update_data_dictionaries:
            merged_dict.update(dictionary)

        set_parts = []
        for key, value in merged_dict.items():
            if isinstance(value, str):
                set_parts.append(f"{key}='{value}'")
            elif value is None:
                set_parts.append(f"{key}=NULL")
            else:
                set_parts.append(f"{key}={value}")

        set_clause = ', '.join(set_parts)

        query = f"UPDATE {self.db_name} SET {set_clause} WHERE {where_clause}"
        try:
            self.cursor.execute(query)
            self.conn.commit()
            print("Data updated successfully.")
            return "Data updated successfully."

        except Exception as e:
            self.conn.rollback()
            print(f'Update failed, rolled back. Error: {e}')
            return f'Update failed, rolled back. Error: {e}'


if __name__ == "__main__":
    ...
    # Example instantiation of the SQLiteHelper class. Creates DB files/table if they don't exist
    # table = SQLiteHelper('tableconfig.ini', 'testtable')

    # Example of data insertion
    # table.insert_data(query_columns=('Name', 'age'), query_values=('tester', 3))

    # Example of data deletion
    # table.delete_data('age', '3')

    # Example of updating data
    # table.update_data([{'name':'testing'}], 'age=2')

    # Example of selection data
    # table.select_data('name, age')
