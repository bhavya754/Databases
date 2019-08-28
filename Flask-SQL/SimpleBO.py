import pymysql
import json

cnx = pymysql.connect(host='localhost',
                              user='dbuser',
                              password='dbuser',
                              db='lahman2017',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result


def find_people_by_primary_key(table, primary_key):
    q = "select * from " + table + " where playerid = %s"
    result = run_q(q, (primary_key), True)
    return result

def find_by_primary_key(table, primary_key):
    t={}
    wc = templateToWhereClause(t)
    q = "select * from " + table + " " + wc
    result = run_q(q, (primary_key), True)
    return result



def find_by_template(table, template, fields=None,offset=None, limit=None):
    result = ""
    if offset==None:
        offset=0
    if limit==None:
        limit=10
    wc = templateToWhereClause(template)
    if fields!=None:
        q = "select " + fields[0] +" from " + table + " " + wc + " limit " + str(limit) + " offset " + str(offset)
    else:
        q= "select * from " + table + " " + wc + " limit " + str(limit) + " offset " + str(offset)
    result = run_q(q, None, True)
    return result

def templateToWhereClause(t):
    s = ""
    
    if t is None:
        return s

    for (k,v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"

    if s != "":
        s = "WHERE " + s;

    return s


def templateToWhereClauseSingle(t):
    s = "" 

    if t is None:
        return s

    for (k,v)in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s;

    return s

def find_by_templateSingle(table, template, fields=None, offset=None, limit=None):
    if offset==None:
        offset=0
    if limit==None:
        limit=10
    wc = templateToWhereClauseSingle(template)
    if fields!=None:
        q = "select " + (",").join(fields) +" from " + table + " " + wc + " limit " + str(limit) + " offset " + str(offset)
    else:
        q= "select * from " + table + " " + wc

    result = run_q(q, None, True)
    return result


def addValues(t):
    kl=','.join(list(t.keys()))
    vl=','.join('"{0}"'.format(w) for w in list(t.values()))
    s="(" + kl + "," + ")"

def updatetable(resource, mapped, body):
    wc=templateToWhereClauseSingle(mapped)
    s="UPDATE " + resource + " SET "
    for key in body.keys():
        s += str(key) + " = " + "\' " + str(body[key]) + "\' " + " ,"
    s= s[:len(s)-1]
    s+= wc
    cursor = cnx.cursor()
    cursor.execute(s)
    cnx.commit()
    return "Update Complete"


def insert(table_name, r):
    try:
        keyList = ', '.join(list(r.keys()))
        valueList = ', '.join('"{0}"'.format(w) for w in list(r.values()))
        s = "(" + keyList + ")" + " VALUES " + "(" + valueList + ")"
        query = "INSERT INTO " + table_name + " " + s + ";"
        primary_key = None
        result = run_q(query, (primary_key), True)
        print(query)
        return result

    except Exception as e:
        print(e)


def get_primary_keys(table, fetch=True):
    query = "show keys from "+ table + " where key_name = 'PRIMARY';"
    cursor = cnx.cursor()
    cursor.execute(query)
    l=None
    if fetch:
        result = cursor.fetchall()
        l=[]
        for elem in result:
            l.append(elem['Column_name'])

    else:
        result = None
    cnx.commit()
    return l

def delete(table_name, t):
    try:
        s = ""
        for key, value in t.items():
            if s != "":
                s += " AND "
            s += key + "='" + value + "'"
        if s != "":
            s = "WHERE " + s;
        query = "DELETE FROM " + table_name + " " + s + ";"
        print("Query = ", query)
        with cnx.cursor() as cursor:
            cursor.execute(query)
        cnx.commit()
        return "Deletion Complete"
    except Exception as e:
        print(e)


def custom_query2(playerid):
    q="select ap.playerID, ap.teamID, ap.yearID, ap.G_all, \
     ba.AB, ba.H as hits, f.A as assists, f.E as errors from Appearances ap inner join batting ba \
     on ap.playerID = ba.playerid and ap.teamId = ba.teamId and ap.yearId = ba.yearId \
      inner join fielding f on ap.playerid = f.playerid and ap.teamId = f.teamId and ap.yearId = f.yearId where ap.playerid='"+playerid+"' ;"   
    result= run_q(q,None,True)
    return result

def custom_query1(playerid):
    q="select a1.playerid as player, a2.playerid as teammate, min(a2.yearId) as firstYear, max(a2.yearid) as lastYear, count(*) as total_appearances \
from Appearances a1 inner join Appearances a2 on a1.teamId=a2.teamId and a1.playerid <> a2.playerid and a1.yearId=a2.yearId \
where a1.playerid = '"+playerid+"' group by a1.playerid, a2.playerid order by a2.playerid"
    result= run_q(q,None,True)
    return result


def custom_query3(teamId, yearId):
    q= "select p.nameLast, p.nameFirst, ap.playerID, ap.teamID, ap.yearID, ap.G_all, ba.AB, ba.H as hits, sum(f.A)\
 as assists, sum(f.E) as errors1 from Appearances ap inner join batting ba on ap.playerID = ba.playerid and ap.teamId = ba.teamId\
  and ap.yearId = ba.yearId Inner join fielding f on ap.playerid = f.playerid and ap.teamId = f.teamId and ap.yearId = f.yearId inner\
   join people p on ap.playerid=p.playerID where ap.teamId='"+teamId+"' and ap.yearId='"+yearId+"' group by p.nameLast, p.nameFirst, ap.playerID,\
    ap.teamID, ap.yearID, ap.G_all, ba.AB, hits order by p.playerid"
    result= run_q(q,None,True)
    return result








