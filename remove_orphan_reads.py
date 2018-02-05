#!/usr/bin/env python

import sys
import multiprocessing as mp
import time
import gzip
from itertools import izip_longest

if len(sys.argv) != 5:
	print "Usage: ./remove_orphan_reads.py <fastq file 1> <fastq file 2> <outfile prefix> <fastq sequence headers>\n"
	quit()

begin = time.time()
output1 = mp.Queue()
output2 = mp.Queue()
input = sys.argv[1]
input2 = sys.argv[2]
out1 = sys.argv[3] + "paired1.fastq"
out2 = sys.argv[3] + "paired2.fastq"
orphans1 = sys.argv[3] + "unpaired1.fastq"
orphans2 = sys.argv[3] + "unpaired2.fastq"
seq_headers = sys.argv[4]

if input.endswith(".gz"):
	original1 = gzip.open(input, "rb")
	original2 = gzip.open(input2, "rb")
else:
	original1 = open(input, "r")
	original2 = open(input2, "r")

dict1 = {}
dict2 = {}
print "Writing the files into two dictionaries..."
start_time = time.time()
for x, y in izip_longest(original1, original2):
	if x != None:
		#print x
		if x.startswith(seq_headers):
			if x.endswith("/1\n"):
				header = x.find("/")
				headerx = x[:header]
			else:
				header = x.find(" ")
				headerx = x[:header]
			dict1[headerx] = ""
		else:
			dict1[headerx] += x
	if y != None:
		if y.startswith(seq_headers):
			if y.endswith("/2\n"):
				header = y.find("/")
				headery = y[:header]
			else:
				header = y.find(" ")
				headery = y[:header]
			dict2[headery] = ""
		else:
			dict2[headery] += y
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)
print len(dict1), len(dict2)

start_time = time.time()
print "Comparing the two dictionaries..."
common = (dict1.viewkeys() & dict2.viewkeys())
uncommon1 = (dict1.viewkeys() - dict2.viewkeys())
uncommon2 = (dict2.viewkeys() - dict1.viewkeys())
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

def write(set, dict, key_suffix, out_file):
	out_file = open(out_file, "w")
	for item in set:
		out_file.write("%s%s\n%s" % (item, key_suffix, dict[item]))
	return

write_processes = ["", "", "", ""]
write_processes[0] = mp.Process(target = write, args = (common, dict1, "/1", out1))
write_processes[1] = mp.Process(target = write, args = (common, dict2,"/2", out2))
write_processes[2] = mp.Process(target = write, args = (uncommon1, dict1,"/1", orphans1))
write_processes[3] = mp.Process(target = write, args = (uncommon2, dict2, "/2", orphans2))

start_time = time.time()
print "Writing the reads to files..."

out1 = open(sys.argv[3] + "paired1.fastq", "w")
out2 = open(sys.argv[3] + "paired2.fastq", "w")
orphans1 = open(sys.argv[3] + "unpaired1.fastq", "w")
orphans2 = open(sys.argv[3] + "unpaired2.fastq", "w")

for item in common:
	out1.write("%s/1\n%s" % (item, dict1[item]))
	out2.write("%s/2\n%s" % (item, dict2[item]))
	del dict1[item]
	del dict2[item]
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

print "Writing the orphan reads to a file..."
start_time = time.time()
for item in dict1:
	orphans1.write("%s/1\n%s" % (item, dict1[item]))
for item in dict2:
	orphans2.write("%s/2\n%s" % (item, dict2[item]))
end_time = time.time()
print "It took %s seconds" % (end_time - start_time)

finish = time.time()
total_time = finish - begin
print "The total time for this script was %s seconds" % total_time
