# -*- coding: utf-8 -*-
"""
Created on Mon May 01 23:31:04 2017

@author: pierreau
"""

import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    

    key = record[0]
    mr.emit_intermediate(key, record)
               
                        
def reducer(key, list_of_values):

     total = 0
     key = list_of_values[0][0]
     for v in list_of_values:
         if total==0 and v[1]==key:
             continue
         if v[0] == key:
             total+=1
     mr.emit((key, total))
          
         

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)