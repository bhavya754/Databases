import sys
sys.path
sys.path.append("../")
import pymysql.cursors
import json
import utils.utils as ut
from utils import dffutils as db
from redis_cache import data_cache

db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.


def set_config():
    """
    Creates the DB connection and sets the global variables.

    :param cfg: Application configuration data.
    :return: None
    """
    global db_schema
    global cnx

    db_params = {
        "dbhost": "localhost",
        "port": 3306,
        "dbname": "lahman2017",
        "dbuser": "dbuser",
        "dbpw": "dbuser",
        "cursorClass": pymysql.cursors.DictCursor,
        "charset": 'utf8mb4'
    }

    db_schema = "lahman2017"

    cnx = db.get_new_connection(db_params)


# Given one of our magic templates, forms a WHERE clause.
# { a: b, c: d } --> WHERE a=b and c=d. Currently treats everything as a string.
# We can fix this by using PyMySQL connector query templates.
def templateToWhereClause(t):
    s = ""
    for k,v in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s;

    return s


# Given a table, template and list of fields. Return the result.
def retrieve_by_template(table, t, fields=None, limit=None, offset=None, orderBy=None,use_cache=True):
    if use_cache == True:
        result=data_cache.check_query_cache(table, t, fields)
        if result is not None:
            print("CACHE HIT")
            return result
        else: 
            print("CACHE MISS")
    original_fields=fields
    if t is not None:
        w = templateToWhereClause(t)
    else:
        w = ""

    if orderBy is not None:
        o = "order by " + ",".join(orderBy['fields']) + " " + orderBy['direction'] + " "
    else:
        o = ""

    if limit is not None:
        w += " LIMIT " + str(limit)
    if offset is not None:
        w += " OFFSET " + str(offset)

    if fields is None:
        fields = " * "
    else:
        fields = " " + ",".join(fields) + " "

    cursor=cnx.cursor()
    q = "SELECT " + fields + " FROM " + table + " " + w + ";"

    r = db.run_q(cnx, q, None, fetch=True, commit=True)
    result = data_cache.add_to_query_cache(table, t, original_fields,r)
    return r











