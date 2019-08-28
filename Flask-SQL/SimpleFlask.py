import json
from flask import Flask
from flask import request
import copy
import SimpleBO
import re

#for pagination, provide the offset and limit values in the initial call 
app = Flask(__name__)

def return_response_page(request, data, offset, limit, split=False):
    if len(data[offset:])>=limit:
        links=[]
        s=str(request.url)
        print(s)
        newurl=re.sub("((&)?offset=[0-9])* (&limit=[0-9])*","&offset="+str(offset+limit) + "&limit="+str(limit),s)
        links.append({"Current page":str(request.url)})
        links.append({"Next page":newurl})
        print(newurl)
        if split:
            data=data[offset:offset+limit]
        return data, links
    else:
        links = []
        links.append({"Current page":str(request.url)})
        links.append({"Next page": "Data over"})
        print(links)
        return data, links

def parse_and_print():
    fields=None
    inargs=None
    body=None
    offset=None
    limit=None
    if request.args is not None:
        inargs=dict(copy.copy(request.args))
        fields = copy.copy(inargs.get('fields', None))
        offset=copy.copy(inargs.get('offset', None))
        limit=copy.copy(inargs.get('limit', None))
        if fields:
            del(inargs['fields'])
        if offset:
            del(inargs['offset'])
        if limit:
            del(inargs['limit'])
    try:
        if request.data:
            body=json.loads(request.data.decode('utf8').replace("'", '"'))
        else:
            body=None
    except Exception as e:
        print("Got exception = ", e)
        body= None
 
    return inargs, fields, body, offset,limit


@app.route('/api/<resource>', methods=['GET','POST'])
def get_resource1(resource):
    try:
        in_args, fields, body, offset, limit = parse_and_print()
        if request.method=='GET':
            if limit == None :
                limit=10
            else:
                limit = int(limit[0])
            if offset== None:
                offset=0
            else:
                offset = int(offset[0])
            result = SimpleBO.find_by_template(resource, in_args, fields,offset,limit)
            res_page=return_response_page(request,result,offset, limit)
            return json.dumps(res_page), 200, {'Content-Type': 'application/json; charset=utf-8'}
        elif request.method=='POST':
            result=SimpleBO.insert(resource, body)
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        return str(e)

@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['POST','GET'])
def getrelated_resource(resource, primary_key,related_resource):
    pk_keys = SimpleBO.get_primary_keys(resource)
    pk_val = primary_key.split("_")
    pk_dict = dict(zip(pk_keys, pk_val))
    in_args, fields,body, offset, limit = parse_and_print()
    new_map={}
    for p in pk_dict:
        new_map[p]=[pk_dict[p]]
    where_clause = {**new_map, **in_args}
    if limit == None :
        limit=10
    else:
        limit = int(limit[0])
    if offset== None:
        offset=0
    else:
        offset = int(offset[0])
    try:
        if request.method=='GET':
            res = SimpleBO.find_by_template(related_resource, where_clause, fields, offset, limit)
            final_res = return_response_page(request, res, offset, limit)
            return json.dumps(final_res), 200, {'Content-Type': 'application/json; charset=utf-8'}
        elif request.method=='POST':
            result=SimpleBO.insert(related_resource, body)
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "Method not implemented",501,{'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        return str(e)


@app.route('/api/<resource>/<primary_key>', methods=['PUT','GET','DELETE'])
def get_resource2(resource, primary_key):
    pk_keys=SimpleBO.get_primary_keys(resource)
    pk_val=primary_key.split("_")
    pk_dict = dict(zip(pk_keys,pk_val))
    try:
        in_args, fields,body, offset, limit = parse_and_print()
        if request.method=='PUT':
            result=SimpleBO.updatetable(resource, pk_dict,body)
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        elif request.method=='GET':
            fields=fields[0].split(",")
            if limit == None :
                limit=10
            else:
                limit = int(limit[0])
            if offset== None:
                offset=0
            else:
                offset = int(offset[0])
            result=SimpleBO.find_by_templateSingle(resource,pk_dict,fields,offset,limit)
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        elif request.method=='DELETE':
            result=SimpleBO.delete(resource, pk_dict)
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "Method not implemented",501,{'Content-Type': 'text/plain; charset=utf-8'}
    except Exception as e:
        return str(e)

@app.route('/api/people/<player_id>/career_stats/', methods=['GET'])
def careerstats(player_id):
    in_args, fields, body, offset, limit = parse_and_print()
    if limit == None :
        limit=10
    else:
        limit = int(limit[0])
    if offset== None:
        offset=0
    else:
        offset = int(offset[0])
    try:
        res= SimpleBO.custom_query1(playerid)
        result,links=return_response_page(request, res, offset,limit, split=True)
        return json.dumps([{"result": result}, {"links": links}]), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        return str(e)

@app.route('/api/teammates/<playerid>')
def teammates(playerid):
    in_args, fields, body, offset, limit = parse_and_print()
    if limit == None :
        limit=10
    else:
        limit = int(limit[0])
    if offset== None:
        offset=0
    else:
        offset = int(offset[0])
    try:
        res= SimpleBO.custom_query1(playerid)
        result,links=return_response_page(request, res, offset,limit, split=True)
        return json.dumps([{"result": result}, {"links": links}]), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        return str(e)

#the path here is changed to roster/<teamid>/<yearid>
@app.route('/api/roster/<teamid>/<yearid>', methods=['GET'])
def get_roster(teamid, yearid):
    in_args, fields, body, offset, limit = parse_and_print()
    if limit == None :
        limit=10
    else:
        limit = int(limit[0])
    if offset== None:
        offset=0
    else:
        offset = int(offset[0])
    try:
        res= SimpleBO.custom_query3(teamid,yearid)
        result,links=return_response_page(request, res, offset,limit, split=True)
        return json.dumps([{"result": result}, {"links": links}]), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        return str(e)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)




