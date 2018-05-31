class JobCluster:
    def __init__(self, jobList, memUtil, cpuUtil):
        self.jobList = jobList
        self.memUtil = memUtil
        self.cpuUtil = cpuUtil

    def getJobList(self):
        return self.jobList

    # the below 2 are the centroids
    def getMemUtil(self):
        return self.memUtil

    def getCpuUtil(self):
        return self.cpuUtil







