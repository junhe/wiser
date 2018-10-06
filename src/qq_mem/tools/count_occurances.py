import sys

overall = 0
with open(sys.argv[1], 'r') as occurrances:
    for line in occurrances:
        if line.strip('\n').isdigit():
            overall += int(line.strip('\n'))

print overall
