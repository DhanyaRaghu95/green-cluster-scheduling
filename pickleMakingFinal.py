import pickle
import csv
from Job import Job
from Machine import Machine

job_dict = pickle.load(open("C:\\Users\\aarti\\PycharmProjects\\CCBD_ML\\another try\\job_dict.p","r"))
machine_dict = pickle.load(open("C:\\Users\\aarti\\PycharmProjects\\CCBD_ML\\another try\\machine_dict.p","r"))
sched_job_list = pickle.load(open("C:\\Users\\aarti\\PycharmProjects\\CCBD_ML\\another try\\schedList.p", "r"))
job_dict_inner = {}
machine_dict_inner = {}
for i in range(len(sched_job_list)):
    job_dict_inner[i] = Job(sched_job_list[i][0], sched_job_list[i][1], sched_job_list[i][2], sched_job_list[i][3], sched_job_list[i][4],None,None,None)

i=0
# load from machine_events .csv file
with open('C:\\Users\\aarti\\PycharmProjects\\CCBD_ML\\another try\\machine_events .csv', 'rb') as csvfile:
    data_reader = csv.reader(csvfile, delimiter=',')
    next(data_reader)
    for row in data_reader:
        machine_dict_inner[i] = Machine(float(row[0]), float(row[1]), float(row[2]),None,None)
        i=i+1
# for jobs
jobc_obj = {}
machinec_obj={}

for i in job_dict.keys():
    for j in job_dict[i]:
        if(i not in jobc_obj.keys()):
            job_dict_inner[j].setClusterNumber(i)
            jobc_obj[i]=[job_dict_inner[j]]
        else:
            job_dict_inner[j].setClusterNumber(i)
            jobc_obj[i].append(job_dict_inner[j])

for i in machine_dict.keys():
    for j in machine_dict[i]:
        if(i not in machinec_obj.keys()):
            machinec_obj[i]=[machine_dict_inner[j]]
        else:
            machinec_obj[i].append(machine_dict_inner[j])

pickle.dump(jobc_obj,open("jobc_obj.p","wb"))
pickle.dump(machinec_obj,open("machinec_obj.p","wb"))
