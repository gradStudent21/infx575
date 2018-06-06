import MapReduce
import sys

"""

"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    
    key = record[1]
    mr.emit_intermediate(key, record)
               
                        
def reducer(key, list_of_values):
 
    
     lineItem = []
     orderItem = []
     
     for v in list_of_values:
         if v[0] == 'order':
             orderItem = v
         else:
             lineItem.append(v)
    
     for item in lineItem:
         mr.emit(orderItem + lineItem)
        

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)