import pickle
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
    #clusterwiseTime[i] = sum([j.totalTime for j in jobc_obj[i]])/float(len(jobc_obj[i]))

job_q.sort(key=lambda x: x.getStartTime())
pickle.dump(job_q, open("final_q.p", "wb"))

# sort based on the startTime of the Jobs(incoming jobs)
# tempCluster.sort(key=lambda x:x.getStartTime())
jobCentroids = pickle.load(open("centroids_job.p", "r"))
machineCentroids = pickle.load(open("centroids_machine.p", "r"))

max_dist_list = []
max_j = 0
max_m = 0
"""for p, j in enumerate(jobCentroids):
    max_dist = 999
    for q, m in enumerate(machineCentroids):
        dist = math.sqrt(
            (j[0] - m[0]) ** 2 + (j[1] - m[1]) ** 2)  # ## Euclidean distance - to find dist btw a jobC and a machineC
        if dist < max_dist:
            max_dist = dist
            max_j = j
            max_m = m
    max_dist_list.append([p, q, max_j, max_m])  # ## for each jobC - a machineC is assigned ( along with jobC-id and machineC-id )
"""
# ##
max_dist_list1 = []
jobsMachine = dict()
"""
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
"""
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
#start_time = time.time()
free_machines = []
for j in machinec_obj:
    free_machines.extend(machinec_obj[j])
print len(free_machines)

print len(job_q)

#for job in job_q:  # each job is a job object
done = 0
unallocated_job = pickle.load(open("not_allocated","r"))
job_ids = [j.jobID for j in unallocated_job]
print job_ids[:10]
for job in job_q:
    if job.jobID in job_ids:
        print "ignored"
        continue
    if done%100 == 0:
        print "Done!",done
    #job = job_q[done]
    #print job.jobID
    global deltaMachines,count
    # get the avg running time for all the done jobs

    min_mem = 999
    min_cpu = 999

    global free_machines
    #print machine_allocated
    for m in machine_allocated:
        #print "m: ",m
        if job.startTime >= m[0].endTime:
            m[1].setCurrCpu(m[1].getCurrCpu() + m[0].getCpuUtil())
            m[1].setCurrMem(m[1].getCurrMem() + m[0].getMemUtil())
            temp_list = machine_job[m[1]]
            if m[0] in machine_job[m[1]]:
                #print "TRUE TRUE"
                temp_list.remove(m[0])
                machine_job[m[1]]=temp_list # freeing the machine of the job completed.
            if m[1] not in free_machines:
                free_machines.append(m[1])
    mac = None
    #min_dist = 999
    #jRatio = job.getMemUtil()/job.getCpuUtil()
    for tempMac in free_machines:
        if tempMac.getCurrCpu() <= 0 or tempMac.getCurrMem() <= 0 :
            continue
        if tempMac.getCurrCpu() >= job.getRemCpu() and tempMac.getCurrMem() >= job.getRemMem():
            mac = tempMac
            done += 1
            break

    global machine_allocated
    if mac:
        mac.setCurrCpu(mac.getCurrCpu() - job.getRemCpu())
        mac.setCurrMem(mac.getCurrMem() - job.getRemMem())
        free_machines.remove(mac)
        machine_allocated.append([job, mac])
        #print "machine allocated job: ",job.jobID

        job_machine[job] = mac  # ##keeps track for a job, which machine it got allocated to
        if mac in machine_job:
            machine_job[mac].append(job)  # ##keeps track for a machine, all the jobs it has got allocated
            #print "machine dict: ",[i.jobID for i in machine_job[mac]]
        else:
            #print "not!"
            machine_job[mac] = [job]
            #print "machine dict: ", [i.jobID for i in machine_job[mac]]
    else:
        count+=1

    #print "count: ",count


end_time = time.time()


print "COUNT: ",count
print "TIME: ",end_time-start_time
print "Done: ",done