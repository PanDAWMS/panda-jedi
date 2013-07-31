from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# class to split job
class JobSplitter:

    # constructor
    def __init__(self):
        pass
        

    # split
    def doSplit(self,taskSpec,inputChunk,siteMapper):
        # return for failure
        retFatal    = self.SC_FATAL,[]
        retTmpError = self.SC_FAILED,[]
        # make logger
        tmpLog = MsgWrapper(logger,'jediTaskID=%s' % taskSpec.jediTaskID)
        tmpLog.debug('start')
        # set maxNumFiles using taskSpec if specified
        maxNumFiles = taskSpec.getMaxNumFilesPerJob()
        # set fsize gradients using taskSpec
        sizeGradients  = taskSpec.getOutDiskSize()
        # set fsize intercepts using taskSpec                
        sizeIntercepts = taskSpec.getWorkDiskSize()
        # walltime
        walltimeIntercepts = taskSpec.walltime
        # number of files per job if defined
        nFilesPerJob = taskSpec.getNumFilesPerJob()
        # number of events per job if defined
        nEventsPerJob = taskSpec.getNumEventsPerJob()
        # grouping with boundaryID
        useBoundary = taskSpec.useGroupWithBoundaryID()
        tmpLog.debug('maxNumFiles={0} sizeGradients={1} sizeIntercepts={2} useBoundary={3}'.format(maxNumFiles,
                                                                                                   sizeGradients,
                                                                                                   sizeIntercepts,
                                                                                                   useBoundary))
        tmpLog.debug('walltimeIntercepts={0} nFilesPerJob={1} nEventsPerJob={2}'.format(walltimeIntercepts,
                                                                                        nFilesPerJob,
                                                                                        nEventsPerJob))
        # split
        returnList = []
        subChunks  = []
        iSubChunks = 0
        nSubChunks = 20
        while True:
            # change site
            if iSubChunks % nSubChunks == 0:
                # append to return map
                if subChunks != []:
                    returnList.append({'siteName':siteName,
                                       'subChunks':subChunks})
                    # reset
                    subChunks = []
                # new candidate   
                siteName = inputChunk.getOneSiteCandidate().siteName
                siteSpec = siteMapper.getSite(siteName)
                # get maxSize if it is set in taskSpec
                maxSize = taskSpec.getMaxSizePerJob()
                if maxSize == None:
                    # use maxwdir as the default maxSize
                    maxSize = siteSpec.maxwdir * 1024 * 1024
                # max walltime      
                maxWalltime = siteSpec.maxtime
                tmpLog.debug('chosen {0}'.format(siteName))
                tmpLog.debug('maxSize={0} maxWalltime={1}'.format(maxSize,maxWalltime))
            # get sub chunk    
            subChunk = inputChunk.getSubChunk(siteName,maxSize=maxSize,
                                              maxNumFiles=maxNumFiles,
                                              sizeGradients=sizeGradients,
                                              sizeIntercepts=sizeIntercepts,
                                              nFilesPerJob=nFilesPerJob,
                                              walltimeIntercepts=walltimeIntercepts,
                                              maxWalltime=maxWalltime,
                                              nEventsPerJob=nEventsPerJob,
                                              useBoundary=useBoundary)
            if subChunk == None:
                break
            # append
            subChunks.append(subChunk)
            iSubChunks += 1
        # append to return map if remain
        if subChunks != []:
            returnList.append({'siteName':siteName,
                               'subChunks':subChunks})
        tmpLog.debug('split to %s subchunks' % iSubChunks)            
        # return
        return self.SC_SUCCEEDED,returnList



Interaction.installSC(JobSplitter)
