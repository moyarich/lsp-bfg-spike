import fcntl
import os
from datetime import *

def Log(filename, msg):
    fp = open(filename, 'a')  
    fcntl.flock(fp, fcntl.LOCK_EX)  
    msg = str(os.getpid()) + " " + datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ") + str(msg)
    fp.write(msg)
    fp.write('\n')
    fp.flush()
    fcntl.flock(fp, fcntl.LOCK_UN)  
    fp.close()
