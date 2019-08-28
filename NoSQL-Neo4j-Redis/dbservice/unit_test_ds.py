import sys
sys.path
sys.path.append("../")
from dbservice import dataservice
import utils.utils as ut
import json
import time  

ut.set_debug_mode(True)
dataservice.set_config()
template1={
 	"nameLast":"Smith"
 }

template2 = {
    "nameLast": "Williams",
    "nameFirst": "Ted"
}

fields = ['playerID', 'nameFirst', 'bats', 'birthCity']


def test_get_resource(resource, template, fields, use_cache):
    result = dataservice.retrieve_by_template(resource, template, fields,use_cache=use_cache)
    return result
    # print("Result = ", json.dumps(result, indent=2))

def run_comparison_test(resource, template, fields, use_cache=True):
	print("\nStartig run without cache ...")
	start_time=time.time()
	for i in range(0,1000):
		r=test_get_resource(resource,template, fields,use_cache=False)
		if i==0:
			ut.debug_message("Result=", r)
	end_time=time.time()
	elapsed_time1=end_time-start_time
	print("elapsed_time = ", elapsed_time1)

	print("\nStartig run with cache ...")
	start_time=time.time()
	for i in range(0,1000):
		r=test_get_resource(resource,template, fields,use_cache=True)
		if i==0:
			ut.debug_message("Result=", r)
	end_time=time.time()
	elapsed_time2=end_time-start_time
	print("elapsed_time = ", elapsed_time2)
	print("\n No cahce/cache ratio = ", elapsed_time1/elapsed_time2)

run_comparison_test("people", template1, fields)
run_comparison_test("people", template2, fields)


# test_get_resource()

def test1():
	tmp={"teamID":"BOS","yearID":"2004","playerID":"ortizda01"}
	fields=["playerID","H","AB","HR"]
	resource = "Batting"
	for i in range(0,3):
		result=dataservice.retrieve_by_template(resource, tmp, fields=fields,use_cache=False)
		print("test1: result [",i,"]", json.dumps(result))

test1()

