# Program to demonstrate discomp7.py example. This program reads stdin
# one line at at time; each line is expected to be a number (as
# string). It sleeps for that time, then writes current time on
# stdout.

import sys
import time

lineno = 0
errors = 0
while True:
    line = sys.stdin.readline()
    if not line:
        break
    lineno += 1
    try:
        n = float(line)
    except:
        errors += 1
        continue
    time.sleep(n)
    print('Line %s - current time is %s' % (lineno, time.asctime()))
    sys.stdout.flush() # flush so output is sent to client immediately
exit(errors)
