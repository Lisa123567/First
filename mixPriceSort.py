#!/usr/bin/python
import sys, time, datetime, os

if len(sys.argv) != 4:
    print "Usage: mixPriceSort.py mixPrice_rates_20180512.log workPwd date"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
WORK_DIR = sys.argv[2]
date = sys.argv[3]

print "Test in progress... INPUT_FILE:%s" %(INPUT_FILE)



in_file = open(INPUT_FILE, 'r')

def clean():
    in_file.close()

def run():
    with in_file as f:
        for row in f:
	    if "DEBUG_LEVEL" in row:
            	word = row.split(",")
	    	firstElement = word[0].split(" ")
            	print " firstElement =  %s" %(firstElement)
            	lpName = firstElement[len(firstElement)-1].replace("[T]","")
	    	lpNameFileRate = WORK_DIR+lpName+"_rate_"+date+".log"
            	if os.path.exists(lpNameFileRate):
   		 	print 'file exists'
    			# some processing
		 	append_write = 'a' # append if already exists
	    	else:
     			print 'file does not exists'
			append_write = 'w' # make a new file if not
            	lpFileRate = open(lpNameFileRate,append_write)
	    	lpFileRate.write(row)
	    	lpFileRate.close()
run();
clean();

