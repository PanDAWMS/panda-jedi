from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

vo = 'atlas'
prodSourceLabel = 'managed'

# get SiteMapper
siteMapper = tbIF.getSiteMapper()

wqMap = tbIF.getWorkQueueMap()

tmpSt,jobStat = tbIF.getJobStatWithWorkQueuePerCloud_JEDI(vo,prodSourceLabel)

from pandajedi.jediorder.JobThrottler import JobThrottler


jt = JobThrottler(vo,prodSourceLabel)
jt.initializeMods(tbIF)

workQueue = wqMap.getQueueWithID(1)
print jt.toBeThrottled(vo,workQueue.queue_type,'ND',workQueue,jobStat)
