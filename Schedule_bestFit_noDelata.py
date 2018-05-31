import pickle
import math
import threading
import time

start_time = time.time()
machine_dict = {}
threads = []
jobc_obj = pickle.load(open("jobc_obj.p", "r"))
machinec_obj = pickle.load(open("machinec_obj.p", "r"))
machine_allocated = []
all_machine_clusters = []  # holds all the cluster numbers for the machines. [0,1,2]


for mc in machinec_obj:
    all_machine_clusters.append(mc)
# print "AMC",all_machine_clusters
#all_machine_clusters = machinec_obj.keys()

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
print "second for loop done"
job_q = []
clusterwiseTime = dict()
for i in jobc_obj:
    job_q.extend(jobc_obj[i])
    #clusterwiseTime[i] = sum([j.totalTime for j in jobc_obj[i]]) / float(len(jobc_obj[i]))

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

###
max_dist_list1 = []
jobsMachine = dict()
p=-1
q=-1
for p,j in enumerate(jobCentroids):  # ## Ratio distance - btw a jobC and machineC
    #p+=1
    jRatio = j[0] / j[1]  # ## ratio - mem : cpu
    min_dist = 999
    for q,m in enumerate(machineCentroids):
        #q+=1
        mRatio = m[0] / m[1]
        if m[0] >= j[0] and m[1] >= j[1] and (
                    abs(jRatio - mRatio) < min_dist):  # ## !!! what if just ratio is bigger !!!
            min_dist = jRatio - mRatio
            max_j = j  # ##can be put outside inner loop
            max_m = m
    #q=0
    max_dist_list1.append([p, q, max_j, max_m])  # ## for every jobC
    jobsMachine[p] = q


# print max_dist_list1
#####################################################################################

# all of them get scheduled on machine cluster 2
count = 0
i = 0

done_jobs_count = 1  # CHECK ThiS ONCE
sum_time = 0.0
machine_allocated = []
time_machine_allocation = []

job_machine = dict()
machine_job = dict()
deltaMachines = dict()
job_pending = []
machine_allocated_list = []
job_not_allocated = []
#start_time = time.time()
# for job in job_q:  # each job is a job object
done = 0
c = 0
for job in job_q:
    print c
    c+=1
    #job = job_q[done]
    global deltaMachines, count,machine_allocated,job_not_allocated
    # get the avg running time for all the done jobs
    #sum_time += (job.endTime - job.startTime)
    # print "sum time: ", sum_time
    #avg_time = sum_time / done_jobs_count  # ## can be put outside the loop
    # cluster centroid metrics
    c_num = job.getClusterNumber()
    c_mem = jobCentroids[c_num][0]
    c_cpu = jobCentroids[c_num][1]

    min_mem = 999
    min_cpu = 999
    # Select a machine from the machine-cluster assigned to the job-cluster to which the job belongs to
    # print jobsMachine[c_num],"HIHI"

    free_machines = machinec_obj[jobsMachine[c_num]]  # we have machines from that cluster
    #free_machines = [item for item in free_machines if item not in machine_allocated_list]
    #free_machines = list(set(free_machines) - set(machine_allocated))
    #print "free_machines: ",len(free_machines)
    # print [i.machineID for i in free_machines[:5]]
    # exit()
    global free_machines
    # free_machines_temp = []
    # free the resources after completion of the job
    for m in machine_allocated:
        #print "m: ",m
        mcpu = m[0].getCpuUtil()
        mmem = m[0].getMemUtil()
        if job.startTime >= m[0].endTime:
            m[1].setCurrCpu(m[1].getCurrCpu() + mcpu)
            m[1].setCurrMem(m[1].getCurrMem() + mmem)
            temp_list = machine_job[m[1]]
            if m[0] in machine_job[m[1]]:
                # print "TRUE TRUE"
                temp_list.remove(m[0])
                machine_job[m[1]] = temp_list  # freeing the machine of the job completed.
            if m[1] not in free_machines:
                free_machines.append(m[1])
    mac = None
    min_ratio_dist = 999
    for tempMac in free_machines:
        tcpu = tempMac.getCurrCpu()
        tmem = tempMac.getCurrMem()
        if tempMac in machine_allocated:
            continue
        if tcpu <= 0 or tmem <= 0:
            continue
        if job.getRemCpu() <= 0:
            jobRatio = 0
        else:
            jobRatio = job.getRemMem() / job.getRemCpu()
        if tcpu <= 0:
            machineRatio = tmem / tcpu
        else:
            machineRatio = 0
        if (abs(jobRatio - machineRatio) < min_ratio_dist) and tcpu >= job.getRemCpu() and tmem >= job.getRemMem():
            min_ratio_dist = abs(jobRatio - machineRatio)
            mac = tempMac
    #print "getting out!"

    #global machine_allocated
    if mac:
        #print "yes!"
        # print "machine allocated job: ",job.jobID
        mac.setCurrCpu(mac.getCurrCpu() - job.getRemCpu())
        mac.setCurrMem(mac.getCurrMem() - job.getRemMem())
        machine_allocated_list.append(mac)
        machine_allocated.append([job, mac])
        free_machines.remove(mac)
        job_machine[job] = mac  # ##keeps track for a job, which machine it got allocated to
        if mac in machine_job:
            machine_job[mac].append(job)  # ##keeps track for a machine, all the jobs it has got allocated
            # print "machine dict: ",[i.jobID for i in machine_job[mac]]
        else:
            machine_job[mac] = [job]
            # print "machine dict: ", [i.jobID for i in machine_job[mac]]
    else:
        job_not_allocated.append(job)
        print "hit"
        count += 1
    #print count

end_time = time.time()
pickle.dump(job_not_allocated,open("not_allocated","wb"))
print "DONE:",done
print "COUNT: ", count
print "TIME: ", end_time - start_time
