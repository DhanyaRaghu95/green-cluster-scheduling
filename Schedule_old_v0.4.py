import pickle
import math
import threading
import time

machine_dict = {}
threads = []
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))
machine_allocated = []
all_machine_clusters = []  # holds all the cluster numbers for the machines. [0,1,2]

for mc in machinec_obj:
    all_machine_clusters.append(mc)
# print "AMC",all_machine_clusters


'''
free_machines1 = machinec_obj[2]
for i in free_machines1:
    machine_dict[i] = []
    machine_dict[i].append(0)
'''
machine_dict = dict()  # key : mach and val : c No.
for mc in machinec_obj:
    for m in machinec_obj[mc]:
        machine_dict[m] = mc

job_q = []
clusterwiseTime = dict()
for i in jobc_obj:
    job_q.extend(jobc_obj[i])
    clusterwiseTime[i] = sum([j.totalTime for j in jobc_obj[i]])/float(len(jobc_obj[i]))

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
        dist = math.sqrt(
            (j[0] - m[0]) ** 2 + (j[1] - m[1]) ** 2)  # ## Euclidean distance - to find dist btw a jobC and a machineC
        if dist < max_dist:
            max_dist = dist
            max_j = j
            max_m = m
    max_dist_list.append(
        [p, q, max_j, max_m])  # ## for each jobC - a machineC is assigned ( along with jobC-id and machineC-id )

# ##
max_dist_list1 = []
jobsMachine = dict()
for p, j in enumerate(jobCentroids):  # ## Ratio distance - btw a jobC and machineC
    jRatio = j[0] / j[1]  # ## ratio - mem : cpu
    min_dist = 999
    for q, m in enumerate(machineCentroids):
        mRatio = m[0] / m[1]
        if m[0] >= j[0] and m[1] >= j[1] and (
                    abs(jRatio - mRatio) < min_dist):  # ## !!! what if just ratio is bigger !!!
            min_dist = jRatio - mRatio
            max_j = j  # ##can be put outside inner loop
            max_m = m
    max_dist_list1.append([p, q, max_j, max_m])  # ## for every jobC
    jobsMachine[p] = q

# print max_dist_list1
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
deltaMachines = dict()
job_pending = []
print "DELTA M:",deltaMachines
for job in job_q:  # each job is a job object
    global deltaMachines
    # get the avg running time for all the done jobs
    sum_time += (job.endTime - job.startTime)
    # print "sum time: ", sum_time
    avg_time = sum_time / done_jobs_count  # ## can be put outside the loop
    # cluster centroid metrics
    c_num = job.getClusterNumber()
    c_mem = jobCentroids[c_num][0]
    c_cpu = jobCentroids[c_num][1]

    min_mem = 999
    min_cpu = 999
    # Select a machine from the machine-cluster assigned to the job-cluster to which the job belongs to
    # print jobsMachine[c_num],"HIHI"

    free_machines = machinec_obj[jobsMachine[c_num]]  # we have machines from that cluster
    #print [i.machineID for i in free_machines[:5]]
    #exit()
    global free_machines
    # free_machines_temp = []
    #print "a"
    for dm in deltaMachines:
        #print "dm:",deltaMachines[dm]
        #print "job: ",job.startTime
        if deltaMachines[dm]<= job.startTime:
            for k in machine_job[dm]:
                if k.endTime <= job.startTime:
                    print "IN!"
                    dm.setCurrCpu(dm.getCurrCpu() + k.getCpuUtil())
                    dm.setCurrMem(dm.getCurrMem() + k.getMemUtil())
            deltaMachines.pop(dm)
            free_machines.append(dm)
            ## free the resources!
            print "DELTA MACHINE FREED"
    #print "b"
    # free the resources after completion of the job
    global machine_allocated
    global machine_job
    for m in machine_allocated:
        #m[0] - job, m[i] = mac
        if job.startTime > m[0].endTime:
            m[1].setCurrCpu(m[1].getCurrCpu() + m[0].getCpuUtil())
            m[1].setCurrMem(m[1].getCurrMem() + m[0].getMemUtil())
            print type(m[0]),type(machine_job[m[1]])
            machine_job[m[1]].remove(m[0]) # freeing the machine of the job completed.
            if len(machine_job[m[1]])>0:
                print "b",m[1],m[0],machine_job[m[1]]
            # ##change job_machine also
            if m[1] not in free_machines and m[1].cpu == m[1].getCurrCpu and m[1].mem == m[1].getCurrMem:
                print "GETTING ADDED TO FREE MACHINES"
                free_machines.append(m[1])
    #print "c"
    mac = None
    min_ratio_dist = 999
    free_machines_temp = free_machines
    for tempMac in free_machines:
        if tempMac.getCurrCpu() <= 0 or tempMac.getCurrMem() <= 0:

            # print [i.machineID for i in deltaMachines.keys()]
            time = []
            ## not entering
            """for j in machine_job[tempMac]:
                #print "J: ",j
                time.append(j.totalTime)"""
            """if tempMac in deltaMachines.keys():
                deltaMachines[tempMac] += float(sum(time)) / len(time)
            else:"""


            continue
        # free_machines_temp.append(tempMac)
        #diff_mem = abs(tempMac.getCurrMem() - job.getRemMem())  # mem < 0
        #diff_cpu = abs(tempMac.getCurrCpu() - job.getRemCpu())
        # ## -ve means they dont have space, should this machine be considered if we are doing only one job to one machine?? ( abs take out! )
        # ;;; if 1 job gets many machines, then, in this loop itself keep subtracting resource requrements from the job and keep collecting machines for this job until the requirements are satisfied

        if job.getRemCpu() <= 0:
            jobRatio = 0
        else:
            jobRatio = job.getRemMem() / job.getRemCpu()
        if tempMac.getCurrCpu <= 0:
            machineRatio = tempMac.getCurrMem() / tempMac.getCurrCpu()
        else:
            machineRatio = 0
        if tempMac.getCurrCpu() >= job.getRemCpu() and tempMac.getCurrMem() >= job.getRemMem() and (
                    abs(jobRatio - machineRatio) < min_ratio_dist):
            min_ratio_dist = abs(jobRatio - machineRatio)
            mac = tempMac
            free_machines_temp.remove(mac)
            #print "GETTING ADDED TO DELTA", len(deltaMachines.keys()), tempMac.machineID
            deltaMachines[tempMac] = clusterwiseTime[job.getClusterNumber()] + job.startTime
    #print "d"
    free_machines = free_machines_temp

    if mac:
        machine_allocated.append([job, mac])
        #print "a",job,mac
        mac.setCurrCpu(mac.getCurrCpu() - job.getRemCpu())
        mac.setCurrMem(mac.getCurrMem() - job.getRemMem())
        job_machine[job] = mac  # ##keeps track for a job, which machine it got allocated to
        if mac in machine_job:
            machine_job[mac].append(job)  ###keeps track for a machine, all the jobs it has got allocated
        else:
            machine_job[mac] = [job]
        """"if mac.getCurrCpu() <= 0 or mac.getCurrMem() <= 0:
            print "GETTING ADDED TO DELTA", len(deltaMachines.keys()), mac.machineID
            # print [i.machineID for i in deltaMachines.keys()]
            time = []
            for j in machine_job[mac]:
                    # print "J: ", j
                time.append(j.totalTime)
                # changed this. we need to add time to the existing value rather than replace it every single time.
            if mac in deltaMachines.keys():
                deltaMachines[mac] += float(sum(time)) / len(time)
            else:
                deltaMachines[mac] = float(sum(time)) / len(time) + job.startTime"""
        #print len(machine_allocated),machine_job[mac]
    #print "e"

                    # time after which mac is free: max(time)
"""
    print "############ JOB ALLOCATION ####################"
print "ID: ", job.jobID
print "Job CPU:", job.cpuUtil
print "Machine ID:", mac.machineID
"""

    # free_machines_temp.remove(mac) #! dont do this - for many jobs on one machine
    # ## instead oftaking out from free list machines completely, we must introduce delta ############################
"""
avg_time = sum_time / done_jobs_count
sum_time = sum(time_machine_allocation)
final_time_taken = sum_time  # * len(machine_allocated)
print "FINAL: ", final_time_taken
print "COUNT", count
machine_dict_final = {}
"""
"""
for mach in machine_dict.keys():
    temp_time = sum_time
    for index, time in enumerate(machine_dict[mach]):
        temp_time = temp_time - time
    machine_dict[mach][0] = temp_time

print "final time taken by machine 0: ", machine_dict
"""
