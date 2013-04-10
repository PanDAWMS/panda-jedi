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
        tmpLog = MsgWrapper(logger,'taskID=%s' % taskSpec.taskID)
        tmpLog.debug('start')
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
            # use maxwdir as the default maxSize
            maxSize = siteSpec.maxwdir * 1024 * 1024
            # set maxNumFiles/maxSize using taskSpec if specified
            # FIXME
            pass
            # set gradients and intercepts using taskSpec
            pass
            # get sub chunk    
            subChunk = inputChunk.getSubChunk(siteName,maxSize=maxSize)
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
