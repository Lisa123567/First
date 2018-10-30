#!/usr/bin/python
import sys, time, datetime

if len(sys.argv) != 4:
    print "Usage: mixPrice.py rates_20180223.log percent logDirectory"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
input_number = sys.argv[2]
WORK_DIR = sys.argv[3]
user_percent = 0.01*float(input_number)
print "Test in progress... INPUT_FILE:%s, delta:%s" %(INPUT_FILE,user_percent)

OUTPUT_FILE_INVERTER = WORK_DIR + "/mixPrice_" + INPUT_FILE

result_file = open(OUTPUT_FILE_INVERTER, 'w+')
result_file.write('### start analyzing:%s ###\n' %(INPUT_FILE))

in_file = open(INPUT_FILE, 'r')

def clean():
    result_file.close()
    in_file.close()

def run():
    with in_file as f:
        for row in f:
            word = row.split(",")
            if len(word) == 9:   # both BID and ASK exists
                if len(word[3]) != len(word[6]):     #if BID lenght diffrent from ASK length something is wrong
		    bid = abs(int(float(word[3])))
                    ask = abs(int(float(word[6])))
                    delta = abs(float(ask-bid)/bid)
                    if delta > user_percent:	
                    	iform = "wrong length: " + row + "\n"
                    	result_file.write(iform)
                else:
                    bid = abs(int(float(word[3])))
                    ask = abs(int(float(word[6])))
		    delta = abs(float(ask-bid)/bid)
		    if delta > user_percent:	
                        iform ="delta over:" + str(delta) + " : "  + row + "\n"
                        result_file.write(iform)
                    else:
                        continue

run();
result_file.write('### stop analyzing:%s ###\n' %(INPUT_FILE))
clean();

