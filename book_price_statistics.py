#!/usr/bin/python
import sys

if len(sys.argv) != 2:
    print "Usage: book_test publisher_currenex_itch_rate_20180212.log"
    sys.exit(1)
INPUT_FILE = sys.argv[1]
print "Test in progress... INPUT_FILE:%s" %(INPUT_FILE)
OUTPUT_FILE_ASK_DIFF_VOL = "tob_ask_vol_changed_" + INPUT_FILE
OUTPUT_FILE_BID_DIFF_VOL = "tob_bid_vol_changed_" + INPUT_FILE
OUTPUT_FILE_ASK_DIFF_PRICE = "tob_ask_price_changed_" + INPUT_FILE
OUTPUT_FILE_BID_DIFF_PRICE = "tob_bid_price_changed_" + INPUT_FILE

dif_vol_bid_file = open(OUTPUT_FILE_BID_DIFF_VOL, 'w+')
#dif_vol_bid_file.write('#This is a report to equivalent price with differences value for bid\n')

dif_vol_ask_file = open(OUTPUT_FILE_ASK_DIFF_VOL, 'w+')
#dif_vol_ask_file.write('#This is a report to equivalent price with differences value for ask\n')

dif_price_bid_file = open(OUTPUT_FILE_BID_DIFF_PRICE, 'w+')
#dif_price_bid_file.write('#This is a report to equivalent price with differences price for bid\n')

dif_price_ask_file = open(OUTPUT_FILE_ASK_DIFF_PRICE, 'w+')
#dif_price_ask_file.write('#This is a report to equivalent price with differences price for ask\n')

in_file = open(INPUT_FILE, 'r')

vol_ask = "vol_ask"
price_ask = "price_ask"
vol_bid = "vol_bid"
price_bid = "price_bid"
currency = "currency"
latestLines = {}


def clean():
    dif_vol_ask_file.close()
    dif_vol_bid_file.close()
    dif_price_ask_file.close()
    dif_price_bid_file.close()
    in_file.close()


def currencyFromLine(line):
    a = line.split("55=")
    b = a[1]
    c = b.split()
    d = c[0]
    return d


def lineParseByCurrency(currencyName, line):
    if "269=" in line:
        words = line.split("269=")
        if (len(words) > 2):
            firstL = words[1]
            secondL = words[2]
            if (firstL.startswith("0") and secondL.startswith("1")):
                obj = {}
                bid = firstL.split()
                if (len(bid) > 2):
                    obj[price_bid] = bid[1]
                    obj[vol_bid] = bid[2]
                ask = secondL.split()
                if (len(ask) > 2):
                    obj[price_ask] = ask[1]
                    obj[vol_ask] = ask[2]
                obj[currency] = currencyName
                return obj
    return None

def run():
    with in_file as f:
        for row in f:
            fixedLine = row.replace(chr(001), " ")
            if "55=" in fixedLine:
                currency = currencyFromLine(fixedLine)
                newLineObject = lineParseByCurrency(currency, fixedLine)
                if newLineObject != None:
                    if currency in latestLines:
                        currentLineObject = latestLines[currency]
                        if "97=Y" in fixedLine:
                            if newLineObject[price_bid] != currentLineObject[price_bid]:
                                dif_price_bid_file.write(fixedLine)
                            else:
                                if newLineObject[vol_bid] != currentLineObject[vol_bid]:
                                    dif_vol_bid_file.write(fixedLine)
                                else:
                                    if newLineObject[price_ask] != currentLineObject[price_ask]:
                                        dif_price_ask_file.write(fixedLine)
                                    else:
                                        if newLineObject[vol_ask] != currentLineObject[vol_ask]:
                                            dif_vol_ask_file.write(fixedLine)
                    latestLines[currency] = newLineObject

run()
clean()
print "end"
