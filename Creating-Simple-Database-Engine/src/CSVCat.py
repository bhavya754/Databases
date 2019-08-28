import pymysql
import csv
import logging
import pymysql.cursors
import json

class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """
        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if column_name != None:
            self.column_name = column_name
        if column_type not in self.column_types:
            return "Column Type should be either text or number"
        else:
            self.column_type = column_type
        self.not_null = not_null

    def __str__(self):
        pass

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the column and it's properties.
        """
        pass


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        if index_type not in self.index_types:
            raise Exception ("Index type should be primary, unique or index")
        self.index_name = index_name
        self.index_type = index_type

class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None, store_table=True):
        """
        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        if cnx is None:
            self.cnx = pymysql.connect(host="localhost",
                                           user="dbuser",
                                           password="dbuser",
                                           db="CSVCatalog",
                                           charset='utf8mb4',
                                           cursorclass=pymysql.cursors.DictCursor)
            cursor = self.cnx.cursor()
        else:
            self.cnx = cnx
        self.t_name = t_name
        self.csv_f = str(csv_f)
        with open(self.csv_f, "r") as f:
            d_reader = csv.DictReader(f)
            self.headers = d_reader.fieldnames
        self.columns = []
        self.column_definitions = column_definitions
        self.index_definitions = index_definitions
        if store_table == True:
            flag = 0
            cursor = self.cnx.cursor()
            q1 = "select t_name from table_def where t_name='"+t_name+"'"
            cursor.execute(q1)
            result = cursor.fetchall()
            if result:
                raise Exception ("Table name already exists")
            else:
                q = "insert into table_def (t_name, path) values ('" + t_name + "'" + " ,'" + csv_f + "')"
                print (q)
                cursor.execute(q)
                self.cnx.commit()
            if column_definitions:
                for i in column_definitions:
                    if i.column_name not in self.headers:
                        raise Exception("Column name is invalid")
                    else:
                        if i.column_name not in self.columns:
                            self.add_column_definition(i)
            if index_definitions:
                for i in index_definitions:
                    self.define_index(i.index_name,i.columns,i.kind)

    def __str__(self):
        pass

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        if c.column_name in self.columns:
            print ("Column name already exists")
            return
        else:
            if c.column_name in self.headers:
                self.columns.append(c.column_name)
                self.column_definitions.append(c)
                q = "insert into col_def(col_name,col_type,not_null,t_name) values ('" + c.column_name + "','" + c.column_type + "','" + str(c.not_null) + "','"+ self.t_name+"')"
                cursor = self.cnx.cursor()
                cursor.execute(q)
                self.cnx.commit()
            else:
                return "Invalid column name"
        return

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        if c in self.columns:
            for i in self.column_definitions:
                if i.column_name == c:
                    self.column_definitions.remove(c)
                    self.columns.remove(c)
                    q = "delete from col_def where col_name = '" + c + "'"
                    #print (q)
                    cursor = self.cnx.cursor()
                    cursor.execute(q)
                    self.cnx.commit()
                    break
        else:
            return "Invalid column name"
        return

    def to_json(self):
        """
        :return: A JSON representation of the table and it's elements.
        """
        return json.dumps(self.describe_table(), indent = 2)


    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        q = "select columns from index_def where t_name = '" + self.t_name + "'"
        cursor = self.cnx.cursor()
        cursor.execute(q)
        ans = cursor.fetchall()

        for c in columns:
            flag = 0
            if c not in self.columns:
                raise Exception ("Invalid column_name")
            for i in ans:
                if c == i:
                    flag = 1
                    q = "update index_def set kind = \"PRIMARY\" where columns = '" + c + "'"
                    break
            if flag == 0:
                q = "insert into index_def (index_name,columns,kind,t_name) values ('PRIMARY','" + c + "','PRIMARY'" + ",'" + self.t_name + "')"
                cursor = self.cnx.cursor()
                cursor.execute(q)
                self.cnx.commit()
        return

    def define_index(self, index_name, columns, kind = "INDEX"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        """
        index_types = ("PRIMARY", "UNIQUE", "INDEX")
        if kind in index_types:
            q1 = "select index_name from index_def"
            cursor = self.cnx.cursor()
            cursor.execute(q1)
            out = cursor.fetchall()
            flag = 0
            for i in out:   ## i is a List
                if i == index_name and index_name != 'PRIMARY':
                    flag == 1
            if flag == 1:
                raise Exception ("Index name should be unique")
            for col in columns:
                q = "insert into index_def (index_name,columns,kind,t_name) values ('" + index_name + "','" + col + "','" + kind + "','" + self.t_name + "')"
                cursor = self.cnx.cursor()
                cursor.execute(q)
                self.cnx.commit()
        else:
            raise Exception ("Index type should be primary, unique or index")

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        q = "delete from index_def where index_name = " + index_name
        cursor = self.cnx.cursor()
        cursor.execute(q)
        self.cnx.commit()
        return

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """

        describe = {}
        cursor = self.cnx.cursor()
        q = "select * from table_def where t_name='"+self.t_name+"'"
        cursor.execute(q)
        l = cursor.fetchall()
        describe["definition"] = l[0]
        self.cnx.commit()

        q = "select * from Col_Def where t_name='"+self.t_name+"'"
        self.cnx.begin()
        cursor = self.cnx.cursor()
        cursor.execute(q)
        describe["columns"] = cursor.fetchall()
        self.cnx.commit()
        q = "select * from Index_Def where t_name='"+self.t_name+"'"
        cursor = self.cnx.cursor()
        cursor.execute(q)

        indexes = {}

        for index in cursor.fetchall():
            if index['index_name'] not in indexes.keys():
                indexes[index['index_name']] = {}
                indexes[index['index_name']]['columns'] = [index['columns']]
                indexes[index['index_name']]['kind'] = index['kind']
            else:
                indexes[index['index_name']]['columns'].append(index['columns'])

        describe["indexes"] = indexes

        return describe


class CSVCatalog:

    def __init__(self, dbhost="localhost", dbport="3306",
                 dbname="CSVCatalog", dbuser="dbuser", dbpw="dbuser", debug_mode=None):
        self.cnx = pymysql.connect(host=dbhost,
                                       user=dbuser,
                                       password=dbpw,
                                       db=dbname,
                                       charset='utf8mb4',
                                       cursorclass=pymysql.cursors.DictCursor)
        cursor = self.cnx.cursor()

    def __str__(self):

        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        self_t = TableDefinition(table_name, file_name, column_definitions, cnx = self.cnx)
        return self_t

    def drop_table(self, table_name):
        q = "delete from table_def where t_name = '" + table_name + "'"
        cursor = self.cnx.cursor()
        cursor.execute(q)
        self.cnx.commit()
        cursor = self.cnx.cursor()
        cursor.execute(q)
        self.cnx.commit()
        q = "delete from index_def where t_name = '" + table_name + "'"
        cursor = self.cnx.cursor()
        cursor.execute(q)
        self.cnx.commit()
        return

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        q1 = "select path from table_def where t_name = '" + table_name + "'"
        cursor = self.cnx.cursor()
        cursor.execute(q1)
        self.file_name = cursor.fetchall()
        q2 = "select * from col_def where t_name = '" + table_name + "'"
        cursor.execute(q2)
        cols = cursor.fetchall()
        self.cols = cols
        self.cd = []
        for i in cols:
            self.cd.append(ColumnDefinition(i["col_name"],i["col_type"],i["not_null"]))
        q3 = "select * from index_def where t_name = '" + table_name + "'"
        cursor.execute(q3)
        indexes = cursor.fetchall()
        self.indexes = []
        for i in indexes:
            self.indexes.append(IndexDefinition(i["index_name"],i["kind"]))
        table = TableDefinition(table_name, self.file_name[0]['path'],self.cd,self.indexes,self.cnx,store_table = False)
        return table

