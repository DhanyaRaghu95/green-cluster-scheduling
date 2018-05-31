import pickle
import math
import threading
import time

machine_dict = {}
threads = []
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))
#machine_dict = pickle.load(open("machine_dict.p", "r"))
# ## Get all machine clusters
all_machine_clusters = []
for mc in machinec_obj:
    all_machine_clusters.append(mc)

'''
free_machines1 = machinec_obj[2]
for i in free_machines1:
    machine_dict[i] = []
    machine_dict[i].append(0)
'''
machine_dict = dict() # key : mach and val : c No.
for mc in machinec_obj.keys():
    for m in machinec_obj[mc]:
        machine_dict[m] =  mc
clusterwiseTime = {}
job_q = []
cluterwiseTime = dict()
for i in jobc_obj.keys():
    job_q.extend(jobc_obj[i])
    clusterwiseTime[i]=sum([j.totalTime for j in jobc_obj[i]])


job_q.sort(key=lambda x: x.getStartTime())
pickle.dump(job_q, open("final_q.p", "wb"))

# sort based on the startTime of the Jobs(incoming jobs)
# tempCluster.sort(key=lambda x:x.getStartTime())
jobCentroids = pickle.load(open("centroids_job.p", "r"))
machineCentroids = pickle.load(open("centroids_machine.p", "r"))

max_dist_list = []
max_j = 0
max_m = 0
for p, j in enumerate(jobCentroids):
    max_dist = 999
    for q, m in enumerate(machineCentroids):
        dist = math.sqrt((j[0] - m[0]) ** 2 + (j[1] - m[1]) ** 2)  # ## Euclidean distance - to find dist btw a jobC and a machineC
        if dist < max_dist:
            max_dist = dist
            max_j = j
            max_m = m
    max_dist_list.append([p, q, max_j, max_m])  # ## for each jobC - a machineC is assigned ( along with jobC-id and machineC-id )


# ##
max_dist_list1 = []
jobsMachine = dict()
for p, j in enumerate(jobCentroids):  # ## Ratio distance - btw a jobC and machineC
    jRatio = j[0] / j[1] # ## ratio - mem : cpu
    min_dist = 999
    for q, m in enumerate(machineCentroids):
        mRatio = m[0] / m[1]
        if m[0]>=j[0] and m[1]>=j[1] and (abs(jRatio - mRatio) < min_dist):  # ## !!! what if just ratio is bigger !!!
            min_dist = jRatio - mRatio
            max_j = j # ##can be put outside inner loop
            max_m = m
    max_dist_list1.append([p, q, max_j, max_m]) # ## for every jobC
    jobsMachine[p]=q

#

# ## check it!!!
# eg : jRatio = 2
#
#           mRatio = 4
#           min_dist = -2
#
#           mRatio = 3
#           min_dist = -1 ( its not lesser than -2, so it wont go into for loop
#


# ## what if??? (check!)
#   eg :  specification of mem and cpu of -  j ( 6, 3 ) and m ( 10 , 10 )
#           jRatio = 2 and mRatio = 1
#           Here, jRatio - mRatio < min_dist = False, even though the machine is capable
# so, may be ratio is a bad idea
#

print max_dist_list1
#####################################################################################
dominates = ""
# all of them get scheduled on machine cluster 2
count = 0
i = 0


done_jobs_count = 1  # CHECK ThiS ONCE
sum_time = 0.0
machine_allocated = []
time_machine_allocation = []
start_time = time.time()
job_machine = dict()
machine_job = dict()
for job in job_q:  # each job is a job object
    # get the avg running time for all the done jobs
    sum_time += (job.endTime - job.startTime)
    #print "sum time: ", sum_time
    avg_time = sum_time / done_jobs_count # ## can be put outside the loop
    # cluster centroid metrics
    c_num = job.getClusterNumber()
    c_mem = jobCentroids[c_num][0]
    c_cpu = jobCentroids[c_num][1]

    min_mem = 999
    min_cpu = 999
    # Select a machine from the machine-cluster assigned to the job-cluster to which the job belongs to
    free_machines = all_machine_clusters[jobsMachine[c_num]][:] # we have machines from that cluster
    # ## Subtract the delta machines from this list

    for i in free_machines:
        machine_dict[i] = []
        machine_dict[i].append(0)
    if free_machines == []:
        #time.sleep(avg_time / 10 ** 10)  # delta
        #mac = free_machines[0]
        machine_dict[mac].append((job.endTime - job.startTime)/10 ** 10)
    else:
        # free_machines_temp = free_machines[:]
        mac = free_machines[0]
        machine_dict[mac].append((job.endTime - job.startTime)/10 ** 10)
    #print "call function"
    global free_machines
    free_machines_temp = []
    for tempMac in free_machines:  # since we are considering only cluster number 2
        free_machines_temp.append(tempMac)
        diff_mem = abs(tempMac.getCurrMem() - job.getRemMem())  # mem < 0
        diff_cpu = abs(tempMac.getCurrCpu() - job.getRemCpu())
        # ## -ve means they dont have space, should this machine be considered if we are doing only one job to one machine?? ( abs take out! )
        # ;;; if 1 job gets many machines, then, in this loop itself keep subtracting resource requrements from the job and keep collecting machines for this job until the requirements are satisfied
        min_ratio_dist = 999
        jobRatio = job.getRemMem() / job.getRemCpu()
        machineRatio = tempMac.getCurrMem() / tempMac.getCurrCpu()
        if tempMac.getCurrCpu()>=job.getRemCpu() and tempMac.getCurrMem()>=job.getRemMem() and (abs(jobRatio - machineRatio) < min_ratio_dist):
            #if diff_mem <= min_mem and diff_cpu < min_cpu: # ## changed a little to optimise, i.e., take into consideration the cpu space also after taking care of mem part
                min_mem = diff_mem
                min_cpu = diff_cpu
                mac = tempMac
    free_machines = free_machines_temp
    #print "returning from call function"
    # now we have a best fit free machine
    # full block taken outside


    # ##After getting the machine, alloate it - put in allocated list ; give away resources from mac to job
    # ##The machine is not really staying busy anytime, ie, when a job gets assigned, another job can immediately be assigned on the same machine, ( this has to be changed when we include 'delta' concept
    machine_allocated.append(mac)
    mac.setCurrCpu(mac.getCurrCpu()-job.getRemCpu())
    mac.setCurrMem(mac.getCurrMem()-job.getRemMem())
    #free_machines_temp.remove(mac) #! dont do this - for many jobs on one machine
    # ## instead oftaking out from free list machines completely, we must introduce delta ############################

    job_machine[job]=mac  # ##keeps track for a job, which machine it got allocated to
    if mac in machine_job.keys():
        machine_job[mac].append(job) # ##keeps track for a machine, all the jobs it has got allocated
    else:
        machine_job[mac] = [job]




    ## are we making an assumption that the delta machine, after time delta, becomes a free machine totally? OR
    ## is it partially free?

    ## also, how do we calculate the end of time to say that now delta is over and it can become a free machine now?
    ## we can set a threshold which says that if after delta, avg_cpu and avg_mem is still available on that delta machine
    ## then make that machine go to free machines.

    ## so everytime a job comes and its requirements are lower than the average calculated requirements, we make it search in the list of delta machines than in that of free machines.

    ## this way, under-utlization of machines also doesnt happen.
    ## but the issue is, how to do we "simulate" the completion of time "delta" in order to start transeferring the machine to the list of free machines. And what threshold should we keep in order to queue the jobs on the delta machines rather than searching in the list of free machines.

# ## Utilization

    # in case job requires more than one machine:
    if (mac.getCurrCpu() - job.getRemCpu()) < 0:
        count += 1
        #job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        ic = 1
        #continue
    elif (mac.getCurrMem() - job.getRemMem()) < 0:
        count += 1
        # job.setRemCpu(job.getRemCpu() - mac.getCurrCpu())
        # job.setRemMem(job.getRemMem() - mac.getCurrMem())
        im = 1
        continue
    else:
        t = threading.Thread(target=worker, args=(job, dominates, mac))
        threads.append(t)
        t.start()
        print "MachineID:", mac.machineID
        print "Job Resource:", job.getCpuUtil()
        print "total resource unused: ", mac.getCurrCpu() - job.getCpuUtil()
    # t.join()
    # if ic == 1 or im == 1:
    # print "continued....."
    # callFunction_incomplete(job)
    i += 1
    done_jobs_count += 1
    print i
    ic = 0
    im = 0
    free_machines = free_machines_temp


    # Allocating the job to a machine



avg_time = sum_time / done_jobs_count
sum_time = sum(time_machine_allocation)
final_time_taken = sum_time  # * len(machine_allocated)
print "FINAL: ", final_time_taken
print "COUNT", count
print "AVERAGE: ",avg_time
machine_dict_final = {}
"""
for mach in machine_dict.keys():
    temp_time = sum_time
    for index,time in enumerate(machine_dict[mach]):
        temp_time = temp_time-time
    machine_dict[mach][0] = temp_time
    """


print "final time taken by machine 0: ",machine_dict
