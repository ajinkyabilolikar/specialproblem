import os
import time
import sys
import saga
import random
from pilot import PilotComputeService, ComputeDataService, State


### This is the number of jobs you want to run
NUMBER_JOBS=16
DATA_SIZE = 1024
#data = [20,30,10,40,80,60,70,50]
COORDINATION_URL = "redis://localhost:6379"
#COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
WORKDIR = os.getenv("HOME") + "/specialproblems/remote"
if __name__ == "__main__":
    total_time = time.time()
    data = []
    for i in range(0,DATA_SIZE):
        data.append(random.randint(1,1048576))

    pilot_compute_service = PilotComputeService(COORDINATION_URL)
    dirname = 'sftp://localhost/%s/' % 'home/ajinkya/specialproblems/remote'
    workdir = saga.filesystem.Directory(dirname, saga.filesystem.CREATE_PARENTS)
#    print os.getcwd()
    local_dir1 = saga.filesystem.File("file://localhost/%s/test1.py"%os.getcwd())	
    local_dir1.copy(workdir.get_url())
    pilot_time = time.time()
    pilot_compute_description = { "service_url": "fork://localhost",
                                  "number_of_processes": 2,
                                  "working_directory": WORKDIR,
                                  "walltime":10,
                                }

    pilot_compute_service.create_pilot(pilot_compute_description)
    print 'time take by pilot',time.time() - pilot_time,"second"
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)

    print ("Finished Pilot-Job setup. Submit compute units")
    
    input_string = ','.join(map(str,data))
    # submit Set A compute units
    all_A_cus = []
    compute_time1 = time.time()
    for i in range(NUMBER_JOBS):
        start = i* int(len(data)/NUMBER_JOBS)
	end = (i+1) * int(len(data)/NUMBER_JOBS) 
# 	print start,end
	file_time = time.time()
	input_file = open('input_%s'%(i),'w')
	
	#Sending input files to the remote machine
	
	

	for element in data[start:end] :
		input_file.write(str(element))
		input_file.write('\n')
#	input_file = open('input_%s'%(i),'w')
#	input_file.write('10')
#	input_file.write('\n')
#	input_file.write('20')
#	input_file.write(str(data[start:end]))
	input_file.close()
	local_dir = saga.filesystem.File("file://localhost/%s/input_%s"%(os.getcwd(),i))
	local_dir.copy(workdir.get_url())
	print'Time taken by file',time.time()-file_time
	#input_string = ','.join(map(str,data[start:end]))
	compute_unit_description = { "executable": "python",
                                     "arguments": [os.getenv("HOME")+'/specialproblems/remote/test1.py',
			             str(i)],
                                    # "environment": ['ENV1=env_arg1','ENV2=env_arg2'+str(i)],
                                     "number_of_processes": 1,
				     "working_directory":WORKDIR,
                                     "output": "A_stdout.txt",
                                     "error": "A_stderr.txt"
                                   }
        compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
        all_A_cus.append(compute_unit) # Store all the compute units.
    compute_time = time.time()       
    compute_data_service.wait()
    
    print 'Total waiting time is ',time.time()-compute_time,"seconds"
    #print ' Total compute unit time is',time.time()-compute_time1,"seconds"
#    print 'Before opening the output file in main'
#    print ' working dir is',workdir.get_url()
#    print 'home dir is ' ,os.getcwd()
    remote_dir = saga.filesystem.File("file://localhost/home/ajinkya/specialproblems/remote/output_")
    remote_dir.copy('sftp://localhost/%s/'%os.getcwd())	
    read_out = open('output_','r')
#	read_int = map(int,read_out)
    new_array = []
    for ti in read_out:
		new_array.append(int(ti))
    minindex = new_array[0]
    #minimum=min(new_array)
    for i in range(0,len(new_array)):
		if(minindex>=new_array[i]):
			minindex = new_array[i]

    print 'the minimum element is',minindex	




     # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.
   
    """  while 1:
        for i in all_A_cus:
           # print i.get_state()
            if i.get_state() == "Done":
                print'inside B'
                compute_unit_description = { "executable": "/bin/echo",
                                             "arguments": ["Hi","$ENV1","$ENV2"+str(i)],
                                             "environment": ['ENV1=task_B:','ENV2=after_task_A'],
                                             "number_of_processes": 1,
                                             "output": "B_stdout.txt",
                                             "error": "B_stderr.txt"
                                           }
                compute_data_service.submit_compute_unit(compute_unit_description)
                all_A_cus.remove(i)

        if len(all_A_cus) == 0:
            break
    # Does this wait refer to both the jobs A and B or only to B?
    print' Wait for set B jobs'
    compute_data_service.wait()"""

    print ("Terminate Pilot Jobs")
    compute_data_service.cancel()
    pilot_compute_service.cancel()
    print'Total time taken is',time.time()-total_time,'seconds'
    remote_dir1 = saga.filesystem.File("file://localhost/home/ajinkya/specialproblems/remote/time")
    remote_dir1.copy('sftp://localhost/%s/'%os.getcwd())
    read_out1 = open('time','r')
    sums = 0
    new_array2 = []
    for ti in read_out1:
                new_array2.append(float(ti))
    for i in range(len(new_array2)):
	sums =sums + new_array2[i]
    print'the average compute unit time is',(sums/NUMBER_JOBS) 
    print 'the length is ',len(new_array2)

