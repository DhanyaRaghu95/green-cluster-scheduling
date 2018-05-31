class MachineCluster:
    def __init__(self, machineList, mem, cpu):
        self.machineList = machineList
        self.mem = mem
        self.cpu = cpu

    def getMachineList(self):
        return self.machineList

    # the below 2 are the centroids
    def getMem(self):
        return self.mem

    def getCpu(self):
        return self.cpu







