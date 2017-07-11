from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediorder.JobThrottler import JobThrottler

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

vo = 'atlas'
prodSourceLabel = 'managed'
resourceType = 'MCORE'
cloud = 'WORLD'

# get SiteMapper
siteMapper = tbIF.getSiteMapper()
wqMap = tbIF.getWorkQueueMap()

jt = JobThrottler(vo, prodSourceLabel)
jt.initializeMods(tbIF)

workQueues = wqMap.getAlignedQueueList(vo, prodSourceLabel)
for workQueue in workQueues:
    print jt.toBeThrottled(vo, prodSourceLabel, cloud, workQueue, resourceType)