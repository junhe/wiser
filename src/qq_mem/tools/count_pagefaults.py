import sys


with open(sys.argv[1], 'r') as input_file:
   count = -10
   for line in input_file:
       if count < 0:
           count += 1
           continue
       majflts = line.split()[5]
       count += int(float(majflts))
   print 'overall: ', count, ' page faults'
