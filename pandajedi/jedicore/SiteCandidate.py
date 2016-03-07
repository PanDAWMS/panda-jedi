class SiteCandidate(object):
    def __init__(self,siteName):
        # the site name
        self.siteName = siteName
        # the weight for the brokerage
        self.weight = 0
        # the list of files copied from SE disk
        self.localDiskFiles = []
        # the list of files copied from SE tape
        self.localTapeFiles = []
        # the list of files cached in non-SE, e.g. on CVMFS 
        self.cacheFiles = []
        # the list of files read from SE using remote I/O
        self.remoteFiles = []
        # the list of all files
        self.allFiles = None
        # acess
        self.remoteProtocol = None
        # remote source if any
        self.remoteSource = None
        # number of running job
        self.nRunningJobs = None
        # number of queued jobs
        self.nQueuedJobs = None
        # number of assigned jobs
        self.nAssignedJobs = None



    # get locality of a file
    def getFileLocality(self,fileSpec):
        for tmpFileSpec in self.localDiskFiles:
            if tmpFileSpec.fileID == fileSpec.fileID:
                return 'localdisk'
        for tmpFileSpec in self.localTapeFiles:
            if tmpFileSpec.fileID == fileSpec.fileID:
                return 'localtape'
        for tmpFileSpec in self.cacheFiles:
            if tmpFileSpec.fileID == fileSpec.fileID:
                return 'cache'
        for tmpFileSpec in self.remoteFiles:
            if tmpFileSpec.fileID == fileSpec.fileID:
                return 'remote'
        return None



    # add available files
    def addAvailableFiles(self,fileList):
        if self.allFiles == None:
            self.allFiles = set()
        for tmpFileSpec in fileList:
            self.allFiles.add(tmpFileSpec.fileID)



    # check if file is available
    def isAvailableFile(self,tmpFileSpec):
        # N/A
        if self.allFiles == None:
            return True
        return tmpFileSpec.fileID in self.allFiles

