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


    # get locality of a file
    def getFileLocality(self,tmpFileSpec):
        if tmpFileSpec in self.localDiskFiles:
            return 'localdisk'
        if tmpFileSpec in self.localTapeFiles:
            return 'localtape'
        if tmpFileSpec in self.cacheFiles:
            return 'cache'
        if tmpFileSpec in self.remoteFiles:
            return 'remote'
        return None
