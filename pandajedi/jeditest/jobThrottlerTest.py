from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

vo = 'atlas'
prodSourceLabel = 'managed'

# get SiteMapper
siteMapper = tbIF.getSiteMapper()

wqMap = tbIF.getWorkQueueMap()

tmpSt,jobStat = tbIF.getJobStatWithWorkQueuePerCloud_JEDI(vo,prodSourceLabel)

from pandajedi.jedithrottle.JobThrottler import JobThrottler

jt = JobThrottler(vo,prodSourceLabel)
jt.initialize(tbIF)

print jt.toBeThrottled(vo,'US',wqMap.getQueueWithID(3),jobStat)
