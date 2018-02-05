#!/usr/bin/env python
"""
This script takes in two paired fastq files, and pulls out reads in which one
pair has been removed by quality filtering, etc. The output is two paired read
files and two 'orphan' read files.
"""

import sys
import multiprocessing as mp
import time
import gzip
from itertools import izip_longest

# The fastq sequence header is the beginning of the header line (duh). It's
# usually something like @M02585
if len(sys.argv) != 5:
    print "Usage: ./remove_orphan_reads.py <fastq file 1> <fastq file 2>\
           <outfile prefix> <fastq sequence headers>\n"
    quit()


begin = time.time()
output1 = mp.Queue()
output2 = mp.Queue()
input_read1 = sys.argv[1]
input_read2 = sys.argv[2]
out_paired1 = sys.argv[3] + "paired1.fastq"
out_paired2 = sys.argv[3] + "paired2.fastq"
orphans1 = sys.argv[3] + "unpaired1.fastq"
orphans2 = sys.argv[3] + "unpaired2.fastq"
seq_headers = sys.argv[4]

if input_read1.endswith(".gz"):
    original1 = gzip.open(input_read1, "rb")
    original2 = gzip.open(input_read2, "rb")
else:
    original1 = open(input_read1, "r")
    original2 = open(input_read2, "r")

# Creates the two read dictionaries and populates each with all of the reads
# from each file
read1_dict = {}
read2_dict = {}
print "Writing the files into two dictionaries..."
start_time = time.time()
for x, y in izip_longest(original1, original2):
    if x != None:
        if x.startswith(seq_headers):
            if x.endswith("/1\n"):
                header = x.find("/")
                headerx = x[:header]
            else:
                header = x.find(" ")
                headerx = x[:header]
            read1_dict[headerx] = ""
        else:
            read1_dict[headerx] += x
    if y != None:
        if y.startswith(seq_headers):
            if y.endswith("/2\n"):
                header = y.find("/")
                headery = y[:header]
            else:
                header = y.find(" ")
                headery = y[:header]
            read2_dict[headery] = ""
        else:
            read2_dict[headery] += y
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)
print len(read1_dict), len(read2_dict)

start_time = time.time()
print "Comparing the two dictionaries..."
# Compares the two read dictionaries and puts reads present in both to a
# viewkeys object
common = (read1_dict.viewkeys() & read2_dict.viewkeys())
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

start_time = time.time()
print "Writing the reads to files..."

out_paired1 = open(sys.argv[3] + "paired1.fastq", "w")
out_paired2 = open(sys.argv[3] + "paired2.fastq", "w")
orphans1 = open(sys.argv[3] + "unpaired1.fastq", "w")
orphans2 = open(sys.argv[3] + "unpaired2.fastq", "w")

# Writes reads present in both input files (common) to their files, and then
# deletes them from read1/2_dict
for item in common:
    out_paired1.write("%s/1\n%s" % (item, read1_dict[item]))
    out_paired2.write("%s/2\n%s" % (item, read2_dict[item]))
    del read1_dict[item]
    del read2_dict[item]
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

print "Writing the orphan reads to a file..."
start_time = time.time()
# Writes the reads left in read1/2_dict to their respective orphaned read file
for item in read1_dict:
    orphans1.write("%s/1\n%s" % (item, read1_dict[item]))
for item in read2_dict:
    orphans2.write("%s/2\n%s" % (item, read2_dict[item]))
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

finish = time.time()
total_time = finish - begin
print "The total time for this script was %s seconds" % total_time
