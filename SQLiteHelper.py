import sqlite3
from configparser import ConfigParser
import logging
import re

"""
This module provides a helper class for working with SQLite databases using a configuration file (.ini) 
to define table schema and primary keys.

Example INI format:

    [example_table]
    *id=INTEGER
    name=TEXT
    age=INTEGER

An asterisk (*) denotes the column is part of the primary key.
"""


def _load_config(filename, section):
    """Load the configuration section from an INI file."""

    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]

        return config

    else:
         raise Exception(f'Section {section} not found in the {filename} file')


def _parse_table_config(input_config):
    """Parse the INI table structure into a list of (column_def, is_primary_key) tuples."""
    return_list = []
    for key, value in input_config.items():
        return_list.append((f'{key.replace("*", "")} {value}', key[0] == '*'))
    return return_list


def sanitize_identifier(identifier):
    """Helper function to assist with detecting SQL Injection."""

    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier


class SQLiteHelper:

    def __init__(self, db_file, db_name, enable_command_logging=False):
        self.__config = _load_config(db_file, db_name)
        self.__config_list = _parse_table_config(self.__config)
        self.__establish_db_conn(db_name)
        self.db_name = db_name
        self.debug = enable_command_logging
        self.__create_table()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__close()

    def __create_table(self):
        """Create the database table if it does not already exist."""

        table_name = self.db_name
        table_layout = self.__config_list

        __command_string__ = f"""CREATE TABLE IF NOT EXISTS {table_name} ("""
        primary_key_fields = []

        for line in table_layout:
            __command_string__ += f'{line[0]}, '
            if line[1]:  
                primary_key_fields.append(line[0].split(' ')[0])

        if primary_key_fields:  
            __command_string__ += f'PRIMARY KEY ({", ".join(primary_key_fields)})'
        else:
            __command_string__ = __command_string__.rstrip(', ')

        __command_string__ += ')'  

        try:
            self.cursor.execute(__command_string__)
            self.conn.commit()
        except sqlite3.OperationalError as se:
            logging.error(f'Exception Occurred: {se}')

    def __establish_db_conn(self, db_name):
        """Establish a connection to the SQLite database."""

        self.conn = sqlite3.connect(f'{db_name}.db')
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def __close(self):
        """Used to close connection to SQLite DB"""
        if self.conn:
            self.conn.close()

    def execute_query(self, query, params=None) -> tuple[list[dict], bool]:
        """
        Execute a SQL query with optional parameters.

        Args:
            query (str): SQL query to execute.
            params (tuple, optional): Values to bind to query placeholders.

        Returns:
            list[dict]: Query results as a list of dictionaries (rows), or an empty list on error.
        """

        try:
            if params is not None:
                if self.debug:
                    logging.info(f'Attempting to execute:'
                                 f'Query - {query}'
                                 f'Params - {params}')

                self.cursor.execute(query, params)
            else:
                if self.debug:
                    logging.info(f'Attempting to execute: \nQuery - {query}')
                self.cursor.execute(query)
            self.conn.commit()
            result = self.cursor.fetchall()
            return [dict(row) for row in result], True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Exception occurred, rolled back any changes. Error: {e}')
            return [], False

    def select_data(self, selection_items: tuple[str], selection_where=None) -> tuple[list[dict], bool]:
        """
        Select rows from the table.

        Args:
            selection_items (tuple): Tuple of strings for columns to search.
            selection_where (str, optional): WHERE clause to filter results.

        Returns:
            tuple[list[dict], bool]: List of matching rows. Bool for if data search was successful.
        """

        columns = ', '.join(selection_items)
        query = f'SELECT {columns} FROM {self.db_name}'
        if selection_where:
            query += f' WHERE {selection_where}'
        try:
            data = self.execute_query(query)
            logging.info(data)
            if data:
                return data
            else:
                return [], False

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Select failed, rolled back. Error: {e}')
            return [], False

    def insert_data(self, query_columns: tuple[str], query_values: tuple[str]) -> tuple[str, bool]:
        """
        Insert a new row into the table.

        Args:
            query_columns (tuple): Column names for insertion.
            query_values (tuple): Corresponding values to insert.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

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
            logging.info("Data inserted successfully.")
            return "Data inserted successfully.", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Insertion failed, rolled back. Error: {e}')
            return f'Insertion failed, rolled back. Error: {e}', False

    def delete_data(self, column_name, value_to_delete) -> tuple[str, bool]:
        """
        Delete rows matching a specific column value.

        Args:
            column_name (str): Column to match.
            value_to_delete (any): Value that identifies rows to delete.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

        query = f"DELETE FROM {self.db_name} WHERE {column_name} = ?"
        try:
            self.execute_query(query, (value_to_delete,))
            logging.info("Data deleted successfully.")
            return "Data deleted successfully", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Deletion failed, rolled back. Error: {e}')
            return f'Deletion failed, rolled back. Error: {e}', False

    def update_data(self, update_data_dictionaries: dict, where_clause: str) -> tuple[str, bool]:
        """
        Update rows in the table.

        Args:
            update_data_dictionaries (list[dict]): Each dictionary maps column names to updated values.
            where_clause (str): WHERE condition to match rows for update.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

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
            self.execute_query(query)
            logging.info("Data updated successfully.")
            return "Data updated successfully.", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Update failed, rolled back. Error: {e}')
            return f'Update failed, rolled back. Error: {e}', False

    def select_min(self, column_name: str) -> tuple[str, bool]:
        """
        Get the minimum value of a column.

        Args:
            column_name (str): Column to evaluate.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

        query = f"SELECT MIN({column_name}) FROM {self.db_name}"

        try:
            data = self.execute_query(query)
            logging.info(f"Minimum from {column_name}: {data}.")
            return f"Minimum from {column_name}: {data}.", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Selection failed, rolled back. Error: {e}')
            return f'Selection failed, rolled back. Error: {e}', False

    def select_max(self, column_name: str) -> tuple[str, bool]:
        """
        Get the maximum value of a column.

        Args:
            column_name (str): Column to evaluate.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

        query = f"SELECT MAX({column_name}) FROM {self.db_name}"
        try:
            data = self.execute_query(query)
            logging.info(f"Maximum from {column_name}: {data}.")
            return f"Maximum from {column_name}: {data}.", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Selection failed, rolled back. Error: {e}')
            return f'Selection failed, rolled back. Error: {e}', False

    def select_avg(self, column_name: str):
        """
        Get the average value of a column.

        Args:
            column_name (str): Column to evaluate.

        Returns:
            tuple[str, bool]: Success or failure message. Bool for success/failure
        """

        query = f"SELECT AVG({column_name}) FROM {self.db_name}"
        try:
            data = self.execute_query(query)
            logging.info(f"Average from {column_name}: {data}.")
            return f"Average from {column_name}: {data}.", True

        except Exception as e:
            self.conn.rollback()
            logging.error(f'Selection failed, rolled back. Error: {e}')
            return f'Selection failed, rolled back. Error: {e}', False

    def count(self, where_clause: str=None):
        """
        Count the number of rows in the table.

        Args:
            where_clause (str, optional): An optional SQL WHERE clause (without the 'WHERE' keyword)
                to filter the rows being counted. For example: "age > 30".

        Returns:
            int: The number of rows matching the condition. If no condition is provided,
                 returns the total number of rows in the table.
        """

        query = f"SELECT COUNT(*) as total FROM {self.db_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        data = self.execute_query(query)
        return data[0][0]['total'] if data else 0
