from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

siteMapper = tbIF.getSiteMapper()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()
atlasIF = ddmIF.getInterface('atlas')

from pandajedi.jedibrokerage.AtlasProdJobBroker import AtlasProdJobBroker
br = AtlasProdJobBroker(atlasIF,tbIF)

from pandajedi.jedicore.InputChunk import InputChunk

taskID = 1
datasetID = 2
st,taskSpec    = tbIF.getTaskWithID_JEDI(taskID)
st,datasetSpec = tbIF.getDatasetWithID_JEDI(datasetID)
st,fileSpecList = tbIF.getFilesInDatasetWithID_JEDI(taskID,datasetID,2)
for fileSpec in fileSpecList:
    datasetSpec.addFile(fileSpec)
inputChunk = InputChunk(datasetSpec)

st,inputChunk = br.doBrokerage(taskSpec,'US',inputChunk)

from pandajedi.jediorder.JobSplitter import JobSplitter
splitter = JobSplitter()
st,chunks = splitter.doSplit(taskSpec,inputChunk,siteMapper)
