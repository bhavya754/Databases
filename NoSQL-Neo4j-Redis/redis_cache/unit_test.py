import sys
sys.path
sys.path.append("../")
from redis_cache import data_cache
from utils import utils as ut
from dbservice import dataservice as ds 
import json 

ut.set_debug_mode(True)

t = {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}
r = data_cache.compute_key("people", {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}, \
                           ['nameLast', "birthCity"])


def test1():
    data_cache.add_to_cache(r, t)
    print("Added to cache")
test1()

def test2():
    result = data_cache.get_from_cache(r)
    print("Result = ", result)

test2()