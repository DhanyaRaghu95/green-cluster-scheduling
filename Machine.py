class Machine:
    #__slots__ = ["currCpu","currMem","machineID","mem","cpu"]
    def __init__(self, machineID, mem, cpu,currMem,currCpu):
        self.machineID = machineID
        self.mem = mem
        self.cpu = cpu
        self.currMem = self.mem
        self.currCpu = self.cpu

    def getMem(self):
        return self.mem

    def getCpu(self):
        return self.cpu

    def setMem(self,mem):
        self.mem = mem

    def setCpu(self,cpu):
        self.cpu = cpu

    def setCurrMem(self, mem):
        self.currMem = mem

    def setCurrCpu(self, cpu):
        self.currCpu = cpu

    def getCurrMem(self):
        return self.currMem

    def getCurrCpu(self):
        return self.currCpu




