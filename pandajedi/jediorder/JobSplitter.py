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
        tmpLog = MsgWrapper(logger,'<jediTaskID={0} datasetID={1}>'.format(taskSpec.jediTaskID,inputChunk.masterIndexName))
        tmpLog.debug('start')
        if not inputChunk.isMerging:
            # set maxNumFiles using taskSpec if specified
            maxNumFiles = taskSpec.getMaxNumFilesPerJob()
            # set fsize gradients using taskSpec
            sizeGradients  = taskSpec.getOutDiskSize()
            # set fsize intercepts using taskSpec                
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # walltime
            walltimeGradient = taskSpec.walltime
            # number of events per job if defined
            nEventsPerJob = taskSpec.getNumEventsPerJob()
            # number of files per job if defined
            nFilesPerJob = taskSpec.getNumFilesPerJob()
            if nFilesPerJob == None and nEventsPerJob == None and inputChunk.useScout() and not taskSpec.useLoadXML():
                nFilesPerJob = 1
            # grouping with boundaryID
            useBoundary = taskSpec.useGroupWithBoundaryID()
            # fsize intercepts per input size
            sizeGradientsPerInSize = None
            # max primay output size
            maxOutSize = None
        else:
            # set parameters for merging
            maxNumFiles = 50
            sizeGradients = 0
            walltimeGradient = 0
            nFilesPerJob = None
            nEventsPerJob = None
            useBoundary = {'inSplit':3}
            # gradients per input size is 1
            sizeGradientsPerInSize = 1
            # intercepts for libDS
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # mergein of 500MB
            interceptsMergin = 500 * 1024 * 1024
            if sizeIntercepts < interceptsMergin:
                sizeIntercepts = interceptsMergin
            # max output size is 5GB for merging 
            maxOutSize = 5 * 1024 * 1024 * 1024
        tmpLog.debug('maxNumFiles={0} sizeGradients={1} sizeIntercepts={2} useBoundary={3}'.format(maxNumFiles,
                                                                                                   sizeGradients,
                                                                                                   sizeIntercepts,
                                                                                                   useBoundary))
        tmpLog.debug('walltimeGradient={0} nFilesPerJob={1} nEventsPerJob={2}'.format(walltimeGradient,
                                                                                        nFilesPerJob,
                                                                                        nEventsPerJob))
        tmpLog.debug('sizeGradientsPerInSize={0} maxOutSize={1}'.format(sizeGradientsPerInSize,
                                                                        maxOutSize))
        # split
        returnList = []
        subChunks  = []
        iSubChunks = 0
        nSubChunks = 50
        while True:
            # change site
            if iSubChunks % nSubChunks == 0:
                # append to return map
                if subChunks != []:
                    returnList.append({'siteName':siteName,
                                       'subChunks':subChunks,
                                       'siteCandidate':siteCandidate,
                                       })
                    # reset
                    subChunks = []
                # new candidate
                siteCandidate = inputChunk.getOneSiteCandidate()
                siteName = siteCandidate.siteName
                siteSpec = siteMapper.getSite(siteName)
                # get maxSize if it is set in taskSpec
                maxSize = taskSpec.getMaxSizePerJob()
                if maxSize == None:
                    # use maxwdir as the default maxSize
                    maxSize = siteSpec.maxwdir * 1024 * 1024
                # max walltime      
                maxWalltime = siteSpec.maxtime
                # core count
                if siteSpec.coreCount > 0:
                    coreCount = siteSpec.coreCount
                else:
                    coreCount = 1
                tmpLog.debug('chosen {0}'.format(siteName))
                tmpLog.debug('maxSize={0} maxWalltime={1} coreCount={2}'.format(maxSize,maxWalltime,coreCount))
            # get sub chunk
            subChunk = inputChunk.getSubChunk(siteName,maxSize=maxSize,
                                              maxNumFiles=maxNumFiles,
                                              sizeGradients=sizeGradients,
                                              sizeIntercepts=sizeIntercepts,
                                              nFilesPerJob=nFilesPerJob,
                                              walltimeGradient=walltimeGradient,
                                              maxWalltime=maxWalltime,
                                              nEventsPerJob=nEventsPerJob,
                                              useBoundary=useBoundary,
                                              sizeGradientsPerInSize=sizeGradientsPerInSize,
                                              maxOutSize=maxOutSize,
                                              coreCount=coreCount,
                                              tmpLog=tmpLog)
            if subChunk == None:
                break
            # append
            subChunks.append(subChunk)
            iSubChunks += 1
        # append to return map if remain
        if subChunks != []:
            returnList.append({'siteName':siteName,
                               'subChunks':subChunks,
                               'siteCandidate':siteCandidate,
                               })
        tmpLog.debug('split to %s subchunks' % iSubChunks)            
        # return
        return self.SC_SUCCEEDED,returnList



Interaction.installSC(JobSplitter)
