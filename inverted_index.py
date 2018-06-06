# -*- coding: utf-8 -*-
"""
Created on Sun Apr 30 15:56:49 2017

@author: pierreau
"""

import MapReduce
import sys

"""

"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
 
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
 
    list =[]
    for v in list_of_values:
        if v in list:
            continue
        else:
             list.append(v)
    mr.emit((key, list))
    
    
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
