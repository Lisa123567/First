#!/usr/bin/python
import sys, time, datetime

if len(sys.argv) != 3:
    print "Usage: mixPrice.py rates_20180223.log 0.3(%)"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
y = sys.argv[2]
x = 0.01*float(y)
print "Test in progress... INPUT_FILE:%s, delta:%s" %(INPUT_FILE,y)

OUTPUT_FILE_INVERTER = "mixPrice_" + INPUT_FILE

result_file = open(OUTPUT_FILE_INVERTER, 'w+')
result_file.write('# delta more that correcting \n')

in_file = open(INPUT_FILE, 'r')

def clean():
    result_file.close()
    in_file.close()

def run():
    with in_file as f:
        for row in f:
            word = row.split(",")
            if len(word) == 9:
                if len(word[3]) != len(word[6]):
                    iform = "wrong length: " + row + "\n"
                    result_file.write(iform)
                else:
                    bid = abs(int(float(word[3])))
                    delta_b = bid * x
                    ask = abs(int(float(word[6])))
                    delta_a = ask * x
                    delta = abs(bid-ask)
                    if delta_b < delta > delta_a:
                        iform = str(delta) + " : " + str(delta_b) + " : " + str(delta_a) + " : " + row + "\n"
                        result_file.write(iform)
                    else:
                        continue

run();
clean();

