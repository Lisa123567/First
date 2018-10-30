#!/usr/bin/python
import sys, time, datetime

if len(sys.argv) != 3:
    print "Usage: book_delta.py publisher_currenex_itch_rate_20180212.log 150(mlsec)"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
y = sys.argv[2]
x = float(y)
print "Test in progress... INPUT_FILE:%s, delta:%s" %(INPUT_FILE,y)
in_ file = open(INPUT_FILE, 'r')
test = x * 0.001
time = "firstTime"
previous_time = "time"
currency = "currency"

def clean():
    in_file.close()
def currencyFromLine(line):
    a = line.split("55=")
    b = a[1]
    c = b.split()
    d = c[0]
    return d

def run():
    second_time = '42.289458'
    previous_time = float(second_time)
    files = {}
    with in_file as f:
        for row in f:
            fixedLine = row.replace(chr(001), " ")
            if "55=" in fixedLine:
                currency = currencyFromLine(fixedLine)
                currencyOutput = currency.replace("/", "")
                OUTPUT_DELTATIME = "deltatime_" + currencyOutput + "_" + INPUT_FILE
                if not OUTPUT_DELTATIME in files:
                    files[OUTPUT_DELTATIME] = open(OUTPUT_DELTATIME, 'w+')
                deltatime_file = files[OUTPUT_DELTATIME]
                words = fixedLine.split()
                first = words[0]
                words = first.split(":")
                if (len(words) > 2):
                    firstTime = words[2]
                    current_time = float(firstTime)
                    delta = (current_time - previous_time)
                    if delta > test:
                        iform = str(delta) + " " + fixedLine
                        previous_time = current_time
                        deltatime_file.write(iform)
                previous_time = current_time
    for file_path in files:
        files[file_path].close()

run()
clean()
print "end"
find . -size 0 -print0 | xargs -0 rm
