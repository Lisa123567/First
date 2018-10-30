#!/usr/bin/python
import os, time, datetime

INPUT_FILE = str(input("Give me your a LP. The format as 'moex_currency.log': "))
TOM_FILE = "tom" + INPUT_FILE
TOD_FILE = "tod" + INPUT_FILE
TODTOM_FILE = "todtom" + INPUT_FILE
TOM1D_FILE = "tom1d" + INPUT_FILE
print 'Your LP is: %s' %INPUT_FILE

in_file = open(INPUT_FILE, 'r')
tod_file = open(TOD_FILE,'w+')
tom_file = open(TOM_FILE,'w+')
todtom_file = open(TODTOM_FILE,'w+')
tom1d_file = open(TOM1D_FILE,'w+')

def run():
    with in_file as f:
        for row in f:
            if "TOM" in row:
                if "TOD" in row:
                    todtom_file.write(row)
                else:
                    if "TOM1D" in row:
                        tom1d_file.write(row)
                    else:
                        tom_file.write(row)
            if "TOD" in row:
                if "TOM" in row:
                    todtom_file.write(row)
                else:
                    tod_file.write(row)
def clean():
    tod_file.close()
    tom_file.close()
    in_file.close()

run()
clean()

print "end"