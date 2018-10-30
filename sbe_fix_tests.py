#!/usr/bin/python
import sys

#if len(sys.argv) != 2:
#   print "Usage: book_test publisher_currenex_itch_rate_20180212.log"
#    sys.exit(1)
#INPUT_FILE = sys.argv[1]

INPUT_FILE = str(input("part1_publisher_ebsu_rate_20180823.log': "))
print "Test in progress... INPUT_FILE:%s" %(INPUT_FILE)
#
# in_file = open(INPUT_FILE, 'r')
# currency = "currency"
# print "Test in progress... INPUT_FILE:%s" %(INPUT_FILE)
OUTPUT_FILE = "string_" + INPUT_FILE

in_file = open(INPUT_FILE, 'r')
currency = "currency"
time = "LastUpdateTime"
latestLines = {}

output_file = open(OUTPUT_FILE, 'w+')
#output_file.write('#This is a string with key 779 and 269 strings.\n')


def clean():
    output_file.close()
    in_file.close()

def sort():
    output_file.set()

def currencyFromLine(line):
    a = line.split("55=")
    b = a[1]
    c = b.split()
    d = c[0]
    return d

def lineParseByMDEntryType(currency, fixedLine, full_bid, full_offer,full_trade):
    if "269=" in fixedLine:
        bid = fixedLine.count("269=BID")
        if bid > 0:
            bd ="269=BID"
            b=0
            word = fixedLine
            for bd in fixedLine:
                if b < bid:
                    b=b+1
                    b1 = word.split("269=BID")
                    b2 = b1[b]
                    b3 = b2.split()
                    full_bid = "269=0 " + b3[0] + " " + b3[1] + " " + b3[4] + " ; "
                    print full_bid
                    output_file.write(full_bid)
        offer = fixedLine.count("269=OFFER")
        if offer > 0:
            f=0
            fd = "269=OFFER"
            for fd in fixedLine:
                if f < offer:
                    f = f + 1
                    f1 = fixedLine.split("269=OFFER")
                    f2 = f1[f]
                    f3 = f2.split()
                    full_offer = "269=1 " + f3[0] + " " + f3[1] + " " + f3[4] + " ; "
                    print full_offer
                    output_file.write(full_offer)
        trade = fixedLine.count("269=TRADE")
        if trade > 0:
            t=0
            td= "269=TRADE"
            for td in fixedLine:
                if t < trade:
                    t = t + 1
                    t1 = fixedLine.split("269=TRADE")
                    t2 = t1[t]
                    t3 = t2.split()
                    full_trade = "269=2 " + t3[0] + " " + t3[1] + " " + t3[4] + " ; "
                    print full_trade
                    output_file.write(full_trade)
        count= (str(bid) + " " + str(offer) + " " + str(trade) + " " + "\n")
        print count
        output_file.write(count)

def run():
    with in_file as f:
        for row in f:
            fixedLine = row.replace(chr(001), " ")
            if "779=2018" in fixedLine:

                #if "97=Y" not in fixedLine: verify if we have thet 779 already?
                    if "55=" in fixedLine:
                        z = fixedLine.split("779=2018")
                        time = z[1]
                        time1 = time.split()
                        LastUpdateTime = "2018"+time1[0]
                        currency = currencyFromLine(fixedLine)
                        full_bid =1
                        full_offer = 1
                        full_trade =1
                        file_print = (str(LastUpdateTime) + " ; " + str(currency) + " ; ")
                        print LastUpdateTime, currency
                        output_file.write(file_print)
                        lineParseByMDEntryType(currency, fixedLine, full_bid, full_offer, full_trade)
run()
sort()
clean()
print "end"