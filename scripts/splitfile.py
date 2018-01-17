import sys

file_name = sys.argv[1]
print file_name

with open(file_name, 'r') as f:
    lines = f.readlines()

threshold=100000
fileID=0
while fileID<len(lines)/float(threshold):
    with open('fileNo'+str(fileID)+'.txt','w') as currentFile:
        print "Writing to " + str(fileID)
        for currentLine in lines[threshold*fileID:threshold*(fileID+1)]:
            currentFile.write(currentLine)
        fileID+=1
