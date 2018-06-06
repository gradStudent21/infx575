# -*- coding: utf-8 -*-
"""
Created on Wed May 03 09:40:25 2017

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
   
    if record[0] == 'a':
      for i in range(0, 5):
          mr.emit_intermediate((record[1],i),(record[2], record[3]))
    else:
      for j in range(0, 5):
          mr.emit_intermediate((j,record[2]),(record[1], record[3]))

        

def reducer(key, list_of_values):
   
    total  = 0
    matrix_a = {}
    matrix_b = {}
    for v in list_of_values:
        if v[0] in matrix_a:
            matrix_b[v[0]] = v[1]
        else:
            matrix_a[v[0]] = v[1]
    for k in matrix_b.keys():
        total += matrix_b[k] * matrix_a[k]
    mr.emit((key[0],key[1],total))
    
              
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
