#!/usr/bin/env python
# Generate binnum ranges for the use of ES extraction.
# The option '-n' takes the number of nodes involved in the extaction.
# For example, in the case of 5 nodes, the command should be like:
# $ python binnumchunk.py -n5
#

import getopt
import sys


def binnum_chunk(number, start, end):
    total_num = end - start
    copy_number = total_num / number
    quote = total_num % number
    modify = 1
    while (start != end):
        if quote == 0: modify = 0
        yield start, start + copy_number + modify
        start += (copy_number + modify)
        quote -= 1

		
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "s:e:n:")
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    start = 0
    end   = 2 ** 16
    for o, a in opts:
        if o == "-s":
            start = int(a)
        elif o == "-e":
            if a == "max":
                end = 2 ** 16
            else:
                end = int(a)
        elif o == "-n":
            number = int(a)

    for s, e in binnum_chunk(number, start, end):
        print "%s,%s" % (s, e)
