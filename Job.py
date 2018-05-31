class Job:
        def __init__(self,jobID,startTime,endTime,memUtil,cpuUtil,clusterNumber,remCpu,remMem):
            self.jobID = jobID
            self.startTime = startTime
            self.endTime = endTime
            self.totalTime = endTime - startTime
            self.memUtil = memUtil
            self.cpuUtil = cpuUtil
            self.remCpu = cpuUtil
            self.remMem = memUtil
            self.clusterNumber = None

        def getJobID(self):
            return self.jobID

        def getStartTime(self):
            return self.startTime

        def getMemUtil(self):
            return self.memUtil

        def getCpuUtil(self):
            return self.cpuUtil

        def getClusterNumber(self):
            return self.clusterNumber

        def setClusterNumber(self,num):
            self.clusterNumber = num

        def setRemMem(self, mem):
            self.remMem = mem

        def setRemCpu(self, cpu):
            self.remCpu = cpu

        def getRemMem(self):
            return self.remMem

        def getRemCpu(self):
            return self.remCpu