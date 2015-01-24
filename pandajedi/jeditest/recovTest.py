"""
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

taskBufferIF = JediTaskBufferInterface()
taskBufferIF.setupInterface()
"""

from pandajedi.jediconfig import jedi_config

from pandajedi.jedicore import JediTaskBuffer

taskBufferIF= JediTaskBuffer.JediTaskBuffer(None)


from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()
ddmIF = ddmIF.getInterface('atlas')

datasetName = 'valid1.159025.ParticleGenerator_gamma_E100.recon.AOD.e3099_s2082_r6012_tid04635343_00'
lostFiles = set(['AOD.04635343._000012.pool.root.1','AOD.04635343._000015.pool.root.1'])


def resetStatusForLostFileRecovery(datasetName,lostFiles):
    # get jeditaskid
    varMap = {}
    varMap[':type1'] = 'log'
    varMap[':type2'] = 'output'
    varMap[':name1'] = datasetName
    varMap[':name2'] = datasetName.split(':')[-1]
    sqlGI  = 'SELECT jediTaskID,datasetID FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlGI += 'WHERE type IN (:type1,:type2) AND datasetName IN (:name1,:name2) '
    res = taskBufferIF.querySQLS(sqlGI,varMap)
    jediTaskID,datasetID = res[1][0]

    print jediTaskID,datasetID

    # sql to update file status
    sqlUFO  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlUFO += 'SET status=:newStatus '
    sqlUFO += 'WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus AND PandaID=:PandaID '
    # get affected PandaIDs
    sqlLP  = 'SELECT pandaID, FROM {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlLP += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND lfn=:lfn '
    lostPandaIDs = set([])
    nDiff = 0
    for lostFile in lostFiles:
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':datasetID'] = datasetID
        varMap[':lfn'] = lostFile
        print sqlLP+str(varMap)
        res = taskBufferIF.querySQLS(sqlLP,varMap)
        if len(res[1]) > 0:
            nDiff += 1
        for pandaID, in res[1]:
            lostPandaIDs.add(pandaID)
            # update file status to lost
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':PandaID'] = pandaID
            varMap[':type'] = 'output'
            varMap[':newStatus'] = 'lost'
            varMap[':oldStatus'] = 'finished'
            print sqlUFO+str(varMap)
            taskBufferIF.querySQLS(sqlUFO,varMap)

    # update output dataset statistics
    sqlUDO  = 'UPDATE {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlUDO += 'SET nFilesFinished=nFilesFinished-:nDiff '
    sqlUDO += 'WHERE jediTaskID=:jediTaskID AND type=:type '
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':type'] = 'output'
    varMap[':nDiff'] = nDiff
    print sqlUDO+str(varMap)
    res = taskBufferIF.querySQLS(sqlUDO,varMap)

    # get input datasets
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':type'] = 'input'
    sqlGI  = 'SELECT datasetID,datasetName,masterID FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlGI += 'WHERE jediTaskID=:jediTaskID AND type=:type '
    print sqlGI+str(varMap)
    res = taskBufferIF.querySQLS(sqlGI,varMap)
    inputDatasets = {}
    masterID = None
    for datasetID,datasetName,tmpMasterID in res[1]:
        inputDatasets[datasetID] = datasetName
        if tmpMasterID == None:
            masterID = datasetID

    # check if datasets are still available
    lostDatasets = {}
    for datasetID,datasetName in inputDatasets.iteritems():
        tmpList = ddmIF.listDatasets(datasetName)
        if tmpList == []:
            lostDatasets[datasetID] = datasetName


    # sql to update input file status
    sqlUFI  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlUFI += 'SET status=:newStatus,attemptNr=attemptNr+1 '
    sqlUFI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus '
    # sql to get affected inputs
    sqlAI  = 'SELECT fileID,datasetID,lfn FROM {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlAI += 'WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND PandaID=:PandaID '
    # get affected inputs
    datasetCountMap = {}
    lostInputFiles = set()
    for lostPandaID in lostPandaIDs:
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':PandaID'] = lostPandaID
        varMap[':type1'] = 'input'
        varMap[':type2'] = 'pseudo_input'
        print sqlAI+str(varMap)
        res = taskBufferIF.querySQLS(sqlAI,varMap)
        for fileID,datasetID,lfn in res[1]:
            # skip if dataset was already deleted
            if datasetID in lostDatasets:
                if datasetID == masterID:
                    lostInputFiles.add(lfn)
                continue
            # reset file status
            if not datasetID in datasetCountMap:
                datasetCountMap[datasetID] = 0
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            varMap[':fileID'] = fileID
            varMap[':newStatus'] = 'ready'
            varMap[':oldStatus'] = 'finished'
            print sqlUFI+str(varMap)
            resUF = taskBufferIF.querySQLS(sqlUFI,varMap)
            nRow = resUF[1]
            if nRow > 0:
                datasetCountMap[datasetID] += 1

    # update dataset statistics
    sqlUDI  = 'UPDATE {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlUDI += 'SET nFilesUsed=nFilesUsed-:nDiff,nFilesFinished=nFilesFinished-:nDiff '
    sqlUDI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
    for datasetID,nDiff in datasetCountMap.iteritems():
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':datasetID'] = datasetID
        varMap[':nDiff'] = nDiff
        print sqlUDI+str(varMap)
        res = taskBufferIF.querySQLS(sqlUDI,varMap)

    """
    # rename input datasets since deleted names cannot be reused
    sqlUN  = 'UPDATE {0}.JEDI_Datasets SET datasetName=:datasetName '.format(jedi_config.db.schemaJEDI)
    sqlUN += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
    for datasetID,datasetName in lostDatasets.iteritems():
        # look for rev number
        tmpMatch = re.search('\.rcov(\d+)$',datasetName)
        if tmpMatch == None:
            revNum = 0
        else:
            revNum = int(tmpMatch.group(1))
        newDatasetName = re.sub('\.rcov(\d+)$','',datasetName)
        newDatasetName += '.rcov{0}'.format(revNum+1)
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':datasetID'] = datasetID
        varMap[':datasetName'] = newDatasetName
        res = taskBufferIF.querySQLS(sqlUN,varMap)

    # get child task
    sqlGC  = 'SELECT jediTaskID FROM {0}.JEDI_Tasks WHERE parent_tid=:jediTaskID '.format(jedi_config.db.schemaJEDI)
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    res = taskBufferIF.querySQLS(sqlGC,varMap)
    childTaskID, = res[0]
    if not childTaskID in [None,jediTaskID]:
        # get output datasets
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':type1'] = 'output'
        varMap[':type2'] = 'log'
        sqlOD  = 'SELECT datasetID,datasetName FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
        sqlOD += 'WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) '
        res = taskBufferIF.querySQLS(sqlOD,varMap)
        outputDatasets = {}
        for datasetID,datasetName in res[1]:
            # remove scope and extension
            bareName = datasetName.split(':')[-1]
            bareName = re.sub('\.rcov(\d+)$','',bareName)
            outputDatasets[bareName] = {'datasetID':datasetID,
                                        'datasetName':datasetName}
        # get input datasets for child
        varMap = {}
        varMap[':jediTaskID'] = childTaskID
        varMap[':type'] = 'input'
        sqlGI  = 'SELECT datasetName FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
        sqlGI += 'WHERE jediTaskID=:jediTaskID AND type=:type '
        res = taskBufferIF.querySQLS(sqlGI,varMap)
        for datasetName, in res[1]:
            # remove scope and extension
            bareName = datasetName.split(':')[-1]
            bareName = re.sub('\.rcov(\d+)$','',bareName)
            # check if child renamed them
            if bareName in outputDatasets and \
                    datasetName.split(':')[-1] != outputDatasets[bareName]['datasetName'].split(':')[-1]:
                newDatasetName = datasetName
                # remove scope
                if not ':' in outputDatasets[bareName]['datasetName']:
                    newDatasetName = newDatasetName.split(':')[-1]
                # rename output datasets if child renamed them
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID'] = outputDatasets[bareName]['datasetID']
                varMap[':datasetName'] = newDatasetName
                res = taskBufferIF.querySQLS(sqlUN,varMap)
    """
            
    # update task status
    sqlUT  = 'UPDATE {0}.JEDI_Tasks SET status=:newStatus WHERE jediTaskID=:jediTaskID '.format(jedi_config.db.schemaJEDI)
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':newStatus'] = 'finished'
    print sqlUT+str(varMap)
    res = taskBufferIF.querySQLS(sqlUT,varMap)

    # reset parent
    if len(lostInputFiles) != 0:
        resetStatusForLostFileRecovery(inputDatasets[masterID],lostInputFiles)




datasetName = 'valid1.159025.ParticleGenerator_gamma_E100.recon.AOD.e3099_s2082_r6012_tid04635343_00'
lostFiles = set(['AOD.04635343._000012.pool.root.1','AOD.04635343._000015.pool.root.1'])

resetStatusForLostFileRecovery(datasetName,lostFiles)
