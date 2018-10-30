#!/usr/bin/python
INPUT_FDP = str(input("'string_publisher_ebsu_rate_20180912.log': "))
print "Test in progress... INPUT_FILE:%s" %(INPUT_FDP)

INPUT_SBE = str(input("'publisher_ebsu_tosbe_20180912.log': "))
print "Test in progress... INPUT_FILE:%s" %(INPUT_SBE)

OUTPUT_FILE = "compare_" + INPUT_SBE

in_fdp = open(INPUT_FDP, 'r')
in_sbe = open(INPUT_SBE, 'r')
out_compare = open(OUTPUT_FILE, 'w+')

def clean():
    in_fdp.close()
    in_sbe.close()
    out_compare.close()

def run():
    with in_fdp as f1, in_sbe as f2:
        for line1, line2 in zip(f1, f2):
            if line1 != line2:
                common =  (str(line1)  + " ? " + str(line2)  + "\n")
                out_compare.write(common)

run()
clean()

# def run():  for diff as diff in the unix: all information in the outputfile
#     with in_fdp as f1, in_sbe as f2:
#         differ = Differ()
#         for line in differ.compare(f1.readlines(), f2.readlines()):
#             out_compare.write(line)

