#!/usr/bin/python
import os, time, datetime

INPUT_FILE = str(input("Give me your a LP. The format as 'table_20180206.log': "))
OUTPUT_FILE = "out_" + INPUT_FILE
FINPUT_FILE = "finally_" + INPUT_FILE

print 'Your table name is: %s' %INPUT_FILE
in_file = open(INPUT_FILE, 'r')
output_file = open(OUTPUT_FILE,'w+')
finally_file = open(FINPUT_FILE,'w+')

def run():
    with in_file as f:
        for row in f:
            if "alias" in row:
                word = row.split("alias")
                cur = word[1] + "\n"
                if "893=" in cur:
                    cur_tab = cur.split("893=")
                    cur_finally = cur_tab[0] + "\n"
                    finally_file.write(cur_finally)
                output_file.write(cur)

def clean():
    output_file.close()
    finally_file.close()
    in_file.close()

run()
clean()

