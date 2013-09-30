import sys
import os
import time
import saga
from pilot import PilotComputeService, ComputeDataService, State

	 
if __name__ == "__main__":
	#start_time = time.time()	
#input from the main function as arguments

#      	print "Inside main function testing "
	args = sys.argv[1]
#	print "after args"
#        print args
	argint = int(args)
#	argint = 3
#	print 'input_%s'%(argint)
#	print "array initialization"
#	print "array length is"
	#new_array = args.split(',')
	#a = new_array[1]
	#print "value of a is %d " % a
	filename = '/home/ajinkya/specialproblems/remote/input_'+str(argint)
	read_file = open(filename, 'r')
	array = []
	start_time = time.time()
	for line in read_file:
		array.append(int(line))
#        print(read_as_string_array)
#        read_as_int_array = map(int, read_as_string_array)
#        print read_as_int_array
	print array
	args = array #read_as_int_array
#	t = 100000000000	
	#logic for finding the minimum number
	minindex = args[0]
	#start_time = time.time()
	for i in range(0,len(args)):
		if(minindex>=args[i]):
			minindex = args[i]
	#minindex = min(args)
	print "the new minimum is",minindex
	end_time = time.time()-start_time
	
	output_file = open('/home/ajinkya/specialproblems/remote/output_','a')
	output_file.write(str(minindex))
	output_file.write('\n')
	output_file.close()	
	work_dir='sftp://localhost/%s/' % 'home/ajinkya/specialproblems'
	workdir = saga.filesystem.Directory(work_dir, saga.filesystem.CREATE_PARENTS)
	#end_time = time.time()-start_time
	output_file = open('/home/ajinkya/specialproblems/remote/time','a')
        output_file.write(str(end_time))
        output_file.write('\n')
        output_file.close()

#	print 'the working directory is','sftp://localhost/%s/' % 'home/ajinkya/specialproblems'
#	print 'the value is',os.getcwd()
#	print ' the remote directory is',workdir.get_url()
	
	#remote_dir = saga.filesystem.File("file://localhost/%s/output_"%os.getcwd()
	#remote_dir.copy(workdir.get_url())

	
#	tr(b)
   
