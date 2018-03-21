import os

def compare(cpp, java):
    cpp_sen = cpp.strip('\n').split(';')[:-1]
    java_sen = java.strip('\n').split(';')[:-1]
    #print 'cpp:    ', cpp_sen
    #print 'java:   ', java_sen
    common = list(set(cpp_sen).intersection(java_sen))
    #print 'common: ', common
    #print len(common), ' : ', len(java_sen)
    return len(common), len(java_sen)

java_sentences = 0
same_sentences = 0
with open('cpp.out', 'r') as cpp_result:
    with open('java.out', 'r') as java_result:
        cpp_lines = cpp_result.readlines()
        java_lines = java_result.readlines()
        for i in range(0, len(cpp_lines)):
            same, count = compare(cpp_lines[i], java_lines[i])
            java_sentences += count
            same_sentences += same


print same_sentences, ' out of ', java_sentences
