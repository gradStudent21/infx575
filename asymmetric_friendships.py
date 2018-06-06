# -*- coding: utf-8 -*-
"""
Created on Mon May 01 23:31:04 2017

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
    
    for i in range(len(record)):
        mr.emit_intermediate(record[i], record[1-i])
                   
                        
def reducer(key, list_of_values):
           
     for v in list_of_values:
        if list_of_values.count(v) == 1:
            mr.emit((key, v))
      
     
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)