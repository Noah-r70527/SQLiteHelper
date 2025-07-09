# SQLiteHelper
Simple python script for making working with SQLite a bit simpler

This module provides a helper class for working with SQLite databases using a configuration file (.ini) 
to define table schema and primary keys.

Example INI format:

    [example_table]
    *id=INTEGER
    name=TEXT
    age=INTEGER

An asterisk (*) denotes the column is part of the primary key.