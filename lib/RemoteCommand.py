try:
    import pexpect
except ImportError:
    sys.stderr.write('RemoteCommand needs pexpect\n')
    sys.exit(2)

import sys

class RemoteCommand:
	def __init__(self):
		pass

	def ssh_command(self, user, host, password, command):
	    ssh_newkey = 'Are you sure you want to continue connecting'
	    cmd = 'ssh -l %s %s "%s"'%(user, host, command)
	    child = pexpect.spawn(cmd, timeout=600)
	    i = child.expect([pexpect.TIMEOUT, ssh_newkey, 'password: '])
	    # Timeout
	    if i == 0: 
	        print 'ERROR!'
	        print 'SSH could not login. Here is what SSH said:'
	        print child.before, child.after
	        return None
	    # SSH does not have the public key. Just accept it.
	    if i == 1: 
	        child.sendline ('yes')
	        child.expect ('password: ')
	        i = child.expect([pexpect.TIMEOUT, 'password: '])
	        # Timeout
	        if i == 0: 
	            print 'ERROR!'
	            print 'SSH could not login. Here is what SSH said:'
	            print child.before, child.after
	            return None
	    child.sendline(password)
	    child.expect(pexpect.EOF)
	    return child.before

	def scp_command(self, from_user, from_host, from_file, to_user, to_host, to_file, password):
	    child = pexpect.spawn('scp -r %s%s%s %s%s%s' %(from_user, from_host, from_file, to_user, to_host, to_file), timeout=600)
	    child.expect('password:')
	    child.sendline(password)
	    child.expect(pexpect.EOF)

remotecmd = RemoteCommand()