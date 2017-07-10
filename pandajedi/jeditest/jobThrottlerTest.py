from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediorder.JobThrottler import JobThrottler

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

vo = 'atlas'
prodSourceLabel = 'test'
resourceType = 'MCORE'
cloud = 'WORLD'

# get SiteMapper
siteMapper = tbIF.getSiteMapper()
wqMap = tbIF.getWorkQueueMap()

tmpSt, jobStat = tbIF.getJobStatisticsWithWorkQueue_JEDI(vo, prodSourceLabel)
# aggregate statistics by work queue
jobStat_agg = {}
for computingSite, siteMap in jobStat.iteritems():
    for workQueue_ID, workQueueMap in siteMap.iteritems():
        # add work queue
        jobStat_agg.setdefault(workQueue_ID, {})
        for jobStatus, nCount in workQueueMap.iteritems():
            jobStat_agg[workQueue_ID].setdefault(jobStatus, 0)
            jobStat_agg[workQueue_ID][jobStatus] += nCount

jt = JobThrottler(vo, prodSourceLabel)
jt.initializeMods(tbIF)

workQueues = wqMap.getAlignedQueueList(vo, prodSourceLabel)
for workQueue in workQueues:
    print jt.toBeThrottled(vo, prodSourceLabel, cloud, workQueue, resourceType)