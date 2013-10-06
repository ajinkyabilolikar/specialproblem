import os
import time
import sys
import saga
import random
from pilot import PilotComputeService, ComputeDataService, State


### This is the number of jobs you want to run
NUMBER_JOBS=16
DATA_SIZE = 1024
COORDINATION_URL = "redis://localhost:6379"
WORKDIR = os.getenv("HOME") + "/specialproblems/remote"
if __name__ == "__main__":
    total_time = time.time()
    data = []
    for i in range(0,DATA_SIZE):
        data.append(random.randint(1,1048576))

    pilot_compute_service = PilotComputeService(COORDINATION_URL)
    dirname = 'sftp://localhost/%s/' % 'home/ajinkya/specialproblems/remote'
    workdir = saga.filesystem.Directory(dirname, saga.filesystem.CREATE_PARENTS)
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
	file_time = time.time()
	input_file = open('input_%s'%(i),'w')
	
	
	

	for element in data[start:end] :
		input_file.write(str(element))
		input_file.write('\n')
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
    remote_dir = saga.filesystem.File("file://localhost/home/ajinkya/specialproblems/remote/output_")
    remote_dir.copy('sftp://localhost/%s/'%os.getcwd())	
    read_out = open('output_','r')
    new_array = []
    for ti in read_out:
		new_array.append(int(ti))
    minindex = new_array[0]
    for i in range(0,len(new_array)):
		if(minindex>=new_array[i]):
			minindex = new_array[i]

    print 'the minimum element is',minindex	




     # Chaining tasks i.e submit a compute unit, when compute unit from A is successfully executed.
   
