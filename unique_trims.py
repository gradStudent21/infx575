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

    value = record[1]
    value = value[:-10]
    mr.emit_intermediate(value, 1)
                 
                        
def reducer(key, list_of_values):
 
      mr.emit(key)
    
          
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)