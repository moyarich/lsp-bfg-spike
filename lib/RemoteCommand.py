import sys
try:
    import pexpect
except ImportError:
    sys.stderr.write('RemoteCommand needs pexpect\n')
    sys.exit(2)


class RemoteCommand:
	def __init__(self):
		pass

	def ssh_command(self, user, host, password, command):
	    ssh_newkey = 'Are you sure you want to continue connecting'
	    cmd = 'ssh -l %s %s "%s"'%(user, host, command)
	    child = pexpect.spawn(cmd, timeout = 600)
	    try:
	    	i = child.expect([pexpect.TIMEOUT, ssh_newkey, 'password:'])
#	    	print 'in try ssh i = ' + str(i)
	    except Exception,e:
	    	pass
	    else:
		    # Timeout
#		    print 'ssh i = ' + str(i)
		    if i == 0: 
		        print 'ERROR!'
		        print 'SSH could not login. Here is what SSH said:'
		        print child.before, child.after
		        return None
		    # SSH does not have the public key. Just accept it.
		    if i == 1: 
		        child.sendline ('yes')
		        child.expect ('password: ')
		        j = child.expect([pexpect.TIMEOUT, 'password: '])
		        child.sendline(password)
		        # Timeout
		        if j == 0: 
		            print 'ERROR!'
		            print 'SSH could not login. Here is what SSH said:'
		            print child.before, child.after
		            return None
		    if i == 2:
		    	child.sendline(password)
	    
	    child.expect(pexpect.EOF)
	    return child.before

	def scp_command(self, from_user, from_host, from_file, to_user, to_host, to_file, password):
	    child = pexpect.spawn('scp -r %s%s%s %s%s%s' %(from_user, from_host, from_file, to_user, to_host, to_file), timeout = 600)
	    try:
	    	i = child.expect(['password:'])
#	    	print 'in try scp i = ' + str(i)
	    except Exception,e:
	    	pass
	    else:
#		    print 'scp i = ' + str(i)
		    child.sendline(password)
	    child.expect(pexpect.EOF)

remotecmd = RemoteCommand()