import sys
import os
import time
import saga
from pilot import PilotComputeService, ComputeDataService, State

	 
if __name__ == "__main__":
	#start_time = time.time()	
#input from the main function as arguments

	args = sys.argv[1]
	argint = int(args)
	filename = '/home/ajinkya/specialproblems/remote/input_'+str(argint)
	read_file = open(filename, 'r')
	array = []
	start_time = time.time()
	for line in read_file:
		array.append(int(line))
	print array
	args = array #read_as_int_array
	minindex = args[0]
	for i in range(0,len(args)):
		if(minindex>=args[i]):
			minindex = args[i]
	print "the new minimum is",minindex
	end_time = time.time()-start_time
	
	output_file = open('/home/ajinkya/specialproblems/remote/output_','a')
	output_file.write(str(minindex))
	output_file.write('\n')
	output_file.close()	
	work_dir='sftp://localhost/%s/' % 'home/ajinkya/specialproblems'
	workdir = saga.filesystem.Directory(work_dir, saga.filesystem.CREATE_PARENTS)
	output_file = open('/home/ajinkya/specialproblems/remote/time','a')
        output_file.write(str(end_time))
        output_file.write('\n')
        output_file.close()


	
   
