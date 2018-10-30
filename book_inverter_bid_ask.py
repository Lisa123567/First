#!/usr/bin/python
import sys, time, datetime

if len(sys.argv) != 2:
    print "Usage: book_test publisher_currenex_itch_rate_20180212.log"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
print "Test in progress... INPUT_FILE:%s" %(INPUT_FILE)
OUTPUT_FILE_INVERTER = "inverter_" + INPUT_FILE

inverter_file = open(OUTPUT_FILE_INVERTER, 'w+')
inverter_file.write('# ask < bid \n')

in_file = open(INPUT_FILE, 'r')

def clean():
    inverter_file.close()
    in_file.close()

def run():
    with in_file as f:
        for row in f:
            fixedLine = row.replace(chr(001), " ")
            if "269=" in fixedLine:
                words = fixedLine.split("269=")
                if (len(words) > 2):
                    firstL = words[1]
                    secondL = words[2]
                    if (firstL.startswith("0") and secondL.startswith("1")):
                        bid = firstL.split()
                        if (len(bid) > 1):
                            price_bid = bid[1]
                            b = price_bid.split("270=")
                            b_1 = b[1]
                            bid_top = float(b_1)
                        ask = secondL.split()
                        if (len(ask) > 1):
                            price_ask = ask[1]
                            a = price_ask.split("270=")
                            a_1= a[1]
                            ask_top = float(a_1)
                            #print ask_top, bid_top
                            if ask_top < bid_top:
                                inverter_file.write(fixedLine)

run()
clean()
print "end"
