import string

printable = set(string.printable)
line = ''

with open('test.txt', 'r') as in_file:
    line = in_file.readline()
    line = ''.join(filter(lambda x: x in printable, line))
with open('test.out', 'w+') as out_file:
    out_file.write(line)
