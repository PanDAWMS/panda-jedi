import copy

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.InputChunk import InputChunk
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore import JediCoreUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# class to split job
class JobSplitter:

    # constructor
    def __init__(self):
        self.sizeGradientsPerInSizeForMerge = 1.2
        self.interceptsMerginForMerge = 500 * 1024 * 1024



    # split
    def doSplit(self, taskSpec, inputChunk, siteMapper, allow_chunk_size_limit=False):
        # return for failure
        retFatal    = self.SC_FATAL,[]
        retTmpError = self.SC_FAILED,[]
        # make logger
        tmpLog = MsgWrapper(logger,'< jediTaskID={0} datasetID={1} >'.format(taskSpec.jediTaskID,inputChunk.masterIndexName))
        tmpLog.debug('--- start chunk_size_limit={}'.format(allow_chunk_size_limit))
        if not inputChunk.isMerging:
            # set maxNumFiles using taskSpec if specified
            maxNumFiles = taskSpec.getMaxNumFilesPerJob()
            # set fsize gradients using taskSpec
            sizeGradients  = taskSpec.getOutDiskSize()
            # set fsize intercepts using taskSpec
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # walltime
            if not taskSpec.useHS06():
                walltimeGradient = taskSpec.walltime
            else:
                walltimeGradient = taskSpec.getCpuTime()
            # number of events per job if defined
            nEventsPerJob = taskSpec.getNumEventsPerJob()
            # number of files per job if defined
            nFilesPerJob = taskSpec.getNumFilesPerJob()
            if nFilesPerJob is None and nEventsPerJob is None and inputChunk.useScout() \
                    and not taskSpec.useLoadXML() and not taskSpec.respectSplitRule():
                nFilesPerJob = 1
            # grouping with boundaryID
            useBoundary = taskSpec.useGroupWithBoundaryID()
            # fsize intercepts per input size
            sizeGradientsPerInSize = None
            # set max primay output size to avoid producing huge unmerged files
            maxOutSize = 10 * 1024 * 1024 * 1024
            # max size per job
            maxSizePerJob = taskSpec.getMaxSizePerJob()
            if maxSizePerJob is not None:
                maxSizePerJob += InputChunk.defaultOutputSize
            # multiplicity of jobs
            if taskSpec.useJobCloning():
                multiplicity = 1
            else:
                multiplicity = taskSpec.getNumEventServiceConsumer()
            # split with fields
            if taskSpec.getFieldNumToLFN() is not None and taskSpec.useFileAsSourceLFN():
                splitByFields = taskSpec.getFieldNumToLFN()
            else:
                splitByFields = None
        else:
            # set parameters for merging
            maxNumFiles = taskSpec.getMaxNumFilesPerMergeJob()
            sizeGradients = 0
            walltimeGradient = 0
            nFilesPerJob = taskSpec.getNumFilesPerMergeJob()
            nEventsPerJob = taskSpec.getNumEventsPerMergeJob()
            maxSizePerJob = None
            useBoundary = {'inSplit':3}
            multiplicity = None
            # gradients per input size is 1 + margin
            sizeGradientsPerInSize = self.sizeGradientsPerInSizeForMerge
            # intercepts for libDS
            sizeIntercepts = taskSpec.getWorkDiskSize()
            # mergein of 500MB
            interceptsMergin = self.interceptsMerginForMerge
            if sizeIntercepts < interceptsMergin:
                sizeIntercepts = interceptsMergin
            maxOutSize = taskSpec.getMaxSizePerMergeJob()
            if maxOutSize is None:
                # max output size is 5GB for merging by default
                maxOutSize = 5 * 1024 * 1024 * 1024
            # split with fields
            if taskSpec.getFieldNumToLFN() is not None and taskSpec.useFileAsSourceLFN():
                splitByFields = list(range(4+1, 4+1+len(taskSpec.getFieldNumToLFN())))
            else:
                splitByFields = None
        # LB
        respectLB = taskSpec.respectLumiblock()
        # no split for on-site merging
        if taskSpec.on_site_merging():
            no_split = True
        else:
            no_split = False
        # dump
        tmpLog.debug('maxNumFiles={0} sizeGradients={1} sizeIntercepts={2} useBoundary={3}'.format(maxNumFiles,
                                                                                                   sizeGradients,
                                                                                                   sizeIntercepts,
                                                                                                   useBoundary))
        tmpLog.debug('walltimeGradient={0} nFilesPerJob={1} nEventsPerJob={2}'.format(walltimeGradient,
                                                                                        nFilesPerJob,
                                                                                        nEventsPerJob))
        tmpLog.debug('useScout={} isMerging={}'.format(inputChunk.useScout(), inputChunk.isMerging))
        tmpLog.debug('sizeGradientsPerInSize={0} maxOutSize={1} respectLB={2}'.format(sizeGradientsPerInSize,
                                                                                      maxOutSize,
                                                                                      respectLB))
        tmpLog.debug(f'multiplicity={multiplicity} splitByFields={str(splitByFields)} '
                     f'nFiles={inputChunk.getNumFilesInMaster()} no_split={no_split} '
                     f'maxEventsPerJob={taskSpec.get_max_events_per_job()}')
        tmpLog.debug('--- main loop')
        # split
        returnList = []
        subChunks  = []
        iSubChunks = 0
        if inputChunk.useScout() and not inputChunk.isMerging:
            default_nSubChunks = 2
        elif taskSpec.is_hpo_workflow():
            default_nSubChunks = 2
        else:
            default_nSubChunks = 25
        subChunk   = None
        nSubChunks = default_nSubChunks
        strict_chunkSize = False
        tmp_ng_list = []
        while True:
            # change site
            if iSubChunks % nSubChunks == 0 or subChunk == []:
                # append to return map
                if subChunks != []:
                    # get site names for parallel execution
                    if taskSpec.getNumSitesPerJob() > 1 and not inputChunk.isMerging and inputChunk.useJumbo != 'fake':
                        siteName = inputChunk.getParallelSites(taskSpec.getNumSitesPerJob(),
                                                               nSubChunks,[siteName])
                    returnList.append({'siteName':siteName,
                                       'subChunks':subChunks,
                                       'siteCandidate':siteCandidate,
                                       })
                    try:
                        gshare = taskSpec.gshare.replace(' ', '_')
                    except Exception:
                        gshare = None
                    tmpLog.info('split to nJobs=%s at site=%s gshare=%s' % (len(subChunks), siteName,
                                                                                       gshare))
                    # checkpoint
                    inputChunk.checkpoint_file_usage()
                    # reset
                    subChunks = []
                # skip PQs with chunk size limit
                ngList = copy.copy(tmp_ng_list)
                if not allow_chunk_size_limit:
                    for siteName in inputChunk.get_candidate_names():
                        siteSpec = siteMapper.getSite(siteName)
                        if siteSpec.get_job_chunk_size() is not None:
                            ngList.append(siteName)
                # new candidate
                siteCandidate, getCandidateMsg = inputChunk.getOneSiteCandidate(nSubChunks, ngSites=ngList,
                                                                                get_msg=True)
                if siteCandidate is None:
                    tmpLog.debug('no candidate: {0}'.format(getCandidateMsg))
                    break
                tmp_ng_list = []
                siteName = siteCandidate.siteName
                siteSpec = siteMapper.getSite(siteName)
                # set chunk size
                nSubChunks = siteSpec.get_job_chunk_size()
                if nSubChunks is None:
                    nSubChunks = default_nSubChunks
                    strict_chunkSize = False
                else:
                    strict_chunkSize = True
                # directIO
                if not JediCoreUtils.use_direct_io_for_job(taskSpec, siteSpec, inputChunk):
                    useDirectIO = False
                else:
                    useDirectIO = True
                # get maxSize if it is set in taskSpec
                maxSize = maxSizePerJob
                if maxSize is None:
                    # use maxwdir as the default maxSize
                    if not useDirectIO:
                        maxSize = siteCandidate.get_overridden_attribute('maxwdir')
                        if maxSize is None:
                            maxSize = siteSpec.maxwdir
                        if maxSize:
                            maxSize *= 1024 * 1024
                    elif nEventsPerJob is not None or nFilesPerJob is not None:
                        maxSize = None
                    else:
                        maxSize = siteCandidate.get_overridden_attribute('maxwdir')
                        if maxSize is None:
                            maxSize = siteSpec.maxwdir
                        if inputChunk.useScout():
                            maxSize = max(inputChunk.maxInputSizeScouts, maxSize) * 1024 * 1024
                        else:
                            maxSize = max(inputChunk.maxInputSizeAvalanche, maxSize) * 1024 * 1024
                else:
                    # add offset
                    maxSize += sizeIntercepts
                # max disk size
                maxDiskSize = siteCandidate.get_overridden_attribute('maxwdir')
                if maxDiskSize is None:
                    maxDiskSize = siteSpec.maxwdir
                if maxDiskSize:
                    maxDiskSize *= 1024 * 1024
                # max walltime
                maxWalltime = None
                if not inputChunk.isMerging:
                    maxWalltime = taskSpec.getMaxWalltime()
                if maxWalltime is None:
                    maxWalltime = siteSpec.maxtime
                # core count
                if siteSpec.coreCount:
                    coreCount = siteSpec.coreCount
                else:
                    coreCount = 1
                # core power
                corePower = siteSpec.corepower
                if taskSpec.dynamicNumEvents() and siteSpec.mintime:
                    dynNumEvents = True
                else:
                    dynNumEvents = False
                tmpLog.debug('chosen {0} : {1} : nQueue={2} nRunCap={3}'.format(siteName, getCandidateMsg,
                                                                                siteCandidate.nQueuedJobs,
                                                                                siteCandidate.nRunningJobsCap))
                tmpLog.debug('new weight {0}'.format(siteCandidate.weight))
                tmpLog.debug('maxSize={} maxWalltime={} coreCount={} corePower={} '
                             'maxDisk={} dynNumEvents={}'.format(
                    maxSize, maxWalltime, coreCount, corePower, maxDiskSize, dynNumEvents))
                tmpLog.debug('useDirectIO={0} label={1}'.format(useDirectIO, taskSpec.prodSourceLabel))
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
                                              respectLB=respectLB,
                                              corePower=corePower,
                                              dynNumEvents=dynNumEvents,
                                              multiplicity=multiplicity,
                                              splitByFields=splitByFields,
                                              tmpLog=tmpLog,
                                              useDirectIO=useDirectIO,
                                              maxDiskSize=maxDiskSize,
                                              enableLog=True,
                                              no_split=no_split,
                                              min_walltime=siteSpec.mintime,
                                              max_events=taskSpec.get_max_events_per_job())
            if subChunk is None:
                break
            if subChunk != []:
                # append
                subChunks.append(subChunk)
            else:
                tmp_ng_list.append(siteName)
                inputChunk.rollback_file_usage()
            iSubChunks += 1
        # append to return map if remain
        isSkipped = False
        if subChunks != []:
            # skip if chunk size is not enough
            if allow_chunk_size_limit and strict_chunkSize and len(subChunks) < nSubChunks:
                tmpLog.debug('skip splitting since chunk size {} is less than chunk size limit {} at {}'.format(
                    len(subChunks), nSubChunks, siteName))
                inputChunk.rollback_file_usage()
                isSkipped = True
            else:
                # get site names for parallel execution
                if taskSpec.getNumSitesPerJob() > 1 and not inputChunk.isMerging:
                    siteName = inputChunk.getParallelSites(taskSpec.getNumSitesPerJob(),
                                                           nSubChunks,[siteName])
                returnList.append({'siteName':siteName,
                                   'subChunks':subChunks,
                                   'siteCandidate':siteCandidate,
                                   })
                try:
                    gshare = taskSpec.gshare.replace(' ', '_')
                except Exception:
                    gshare = None
                tmpLog.info('split to nJobs=%s at site=%s gshare=%s' % (len(subChunks), siteName,
                                                                                   gshare))
        # return
        tmpLog.debug('--- done')
        return self.SC_SUCCEEDED, returnList, isSkipped



Interaction.installSC(JobSplitter)
