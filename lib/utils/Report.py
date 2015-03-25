import fcntl
import os
from datetime import *

def Report(filename, msg):
    fp = open(filename, 'a')  
    fcntl.flock(fp, fcntl.LOCK_EX)  
    fp.write(str(msg))
    fp.write('\n')
    fp.flush()
    fcntl.flock(fp, fcntl.LOCK_UN)  
    fp.close()
