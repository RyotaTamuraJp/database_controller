# -*- coding: utf-8 -*-
"""
Created on Thu Nov 22 09:45:36 2018

@author: ryota_tamura
"""


from pg import DB


class db_controller(object):
    def __init__(self, dbname, host, port, user, passwd):
        self.db = DB(dbname=dbname, host=host, port=port, user=user,
                     passwd=passwd)

    def get_table_names(self):
        return self.db.get_tables()

    def select(self, table_name, columns):
        cmd = "SELECT {} FROM {}".format(columns, table_name)
        return self.db.query(cmd).getresult()

    def select_where(self, table_name, columns, where_cmd):
        cmd = "SELECT {} FROM {} WHERE {}".format(columns, table_name,
                                                  where_cmd)
        return self.db.query(cmd).getresult()

    def create_table(self, table_name, columns, types):
        contents = ""
        counter = 0
        for col_name, col_type in zip(columns, types):
            counter += 1
            if counter != len(columns):
                contents += "{} {}, ".format(col_name, col_type)
            elif counter == len(columns):
                contents += "{} {}".format(col_name, col_type)
        cmd = "CREATE TABLE {}({})".format(table_name, contents)
        self.db.query(cmd)

    def drop_table(self, table_name):
        cmd = "DROP TABLE {}".format(table_name)
        self.db.query(cmd)

    def insert(self, table_name, columns, values):
        cols = ", ".join(columns)
        vals = ""
        len_values = len(values)
        for i, v in enumerate(values):
            if i+1 != len_values:
                if isinstance(v, str):
                    vals += "'{}', ".format(v)
                else:
                    vals += "{}, ".format(v)
            elif i+1 == len_values:
                if isinstance(v, str):
                    vals += "'{}'".format(v)
                else:
                    vals += "{}".format(v)
        cmd = "INSERT INTO {}({}) VALUES({})".format(table_name, cols, vals)
        self.db.query(cmd)
