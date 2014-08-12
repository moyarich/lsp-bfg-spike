import sys

class QueryFile(object):
    def __init__(self, filename):
        self.fh = open(filename.strip() , 'r')
        self.buff = ""

    def __del__(self):
        self.fh.close()

    def __iter__(self):
        return self

    def next(self):

	# Construct a statement by reading lines until we see
	# a semicolon.
	#
	# There are several special cases:
	#   (1) create (or replace) function: There may be multiple
	#       statements inside $$s.
	#   (2) create (or replace) function: There may be a statement
	#       inside a single quote.
	ignore_semicolon = False
	in_create_func = False
        buff = ""
        
	while True:
            line = self.fh.readline()
            if not line :
                raise StopIteration

	    # strip the whitespace at the beginning of the line
	    stripped_line = line.lstrip()
	   
	    # ignore lines that start with "--" 
	    if stripped_line.startswith("--"):
		continue 
	 
	    # ignore lines that start with "-- using"
	    if line.startswith("-- using"):
		continue

	    if line.startswith("\echo"):
		buff = ""
		continue

	    if (stripped_line.lower().startswith('create function') or
		stripped_line.lower().startswith('create or replace function')):
		in_create_func = True
		num_block_symbols = 0
		find_dolar_symbols = False
		
	    if (in_create_func):
		# Didn't find $$ or '.
		if (line.find('$$') == -1 and line.find('$body$') == -1 and line.find('\'') == -1 and (not ignore_semicolon)):
		    num_block_symbols = 0
		    
		# find $$
		elif line.find('$$') != -1:
		    find_dolar_symbols = True
		    if line.find('$$') == line.rfind('$$'):
			if num_block_symbols == 1:
			    num_block_symbols = 2
			else:
			    num_block_symbols = 1
		    else:
			num_block_symbols = 2

		# find $body$
		elif line.find('$body$') != -1:
		    find_dolar_symbols = True
		    if line.find('$body$') == line.rfind('$body$'):
			if num_block_symbols == 1:
			    num_block_symbols = 2
			else:
			    num_block_symbols = 1
		    else:
			num_block_symbols = 2

		# find '
		elif (not find_dolar_symbols) and line.find('\'') != -1:
		    if line.find('\'') == line.rfind('\''):
			if num_block_symbols == 1:
			    num_block_symbols = 2
			else:
			    num_block_symbols = 1
		    else:
			num_block_symbols = 2

		# set ignore_semicolon
		if num_block_symbols == 0 or num_block_symbols == 1:
		    ignore_semicolon = True
		else:
		    ignore_semicolon = False

	    if (in_create_func and (not ignore_semicolon)):
		in_create_func = False
		find_dolar_symbols = False

	    stripped_line = line.rstrip('\n')
	    stripped_line = stripped_line.rstrip()
	    if stripped_line.endswith(';') and (not ignore_semicolon):
		buff += stripped_line
		# self.execCommand(c1,buff)
		return buff
                
	    else:
		buff += line
            

