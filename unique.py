#!/usr/bin/python
import os, time, datetime

INPUT_FILE = str(input("Give me your a LP. The format as 'finnaly.log': "))
UNIQUE_FILE = "unique_" + INPUT_FILE

print 'Your table name is: %s' %INPUT_FILE
finally_file = open(INPUT_FILE, 'r')
unique_file = open(UNIQUE_FILE,'w+')
def unique():
    lines_seen = set()  # holds lines already seen
    outfile = open(unique_file, 'w+')
    for line in open(finally_file, "r"):
        if line not in lines_seen:  # not a duplicate
            outfile.write(line)
            lines_seen.add(line)
            outfile.writelines(sorted(lines_seen))
            outfile.close()
    outfile.close()

unique()
