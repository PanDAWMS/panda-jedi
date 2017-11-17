from pandajedi.jediconfig import jedi_config

from pandajedi.jedicore import JediTaskBuffer

taskBufferIF= JediTaskBuffer.JediTaskBuffer(None)

from pandaserver.userinterface import Client

from pandajedi.jediddm.DDMInterface import DDMInterface

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

    print 'jediTaskID={0}, datasetName={1}'.format(jediTaskID, datasetName)

    # sql to update file status
    sqlUFO  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlUFO += 'SET status=:newStatus '
    sqlUFO += 'WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus AND PandaID=:PandaID '
    # get affected PandaIDs
    sqlLP  = 'SELECT pandaID FROM {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlLP += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND lfn=:lfn '
    lostPandaIDs = set([])
    nDiff = 0
    for lostFile in lostFiles:
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':datasetID'] = datasetID
        varMap[':lfn'] = lostFile
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
            taskBufferIF.querySQLS(sqlUFO,varMap)

    # update output dataset statistics
    sqlUDO  = 'UPDATE {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlUDO += 'SET nFilesFinished=nFilesFinished-:nDiff '
    sqlUDO += 'WHERE jediTaskID=:jediTaskID AND type=:type '
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':type'] = 'output'
    varMap[':nDiff'] = nDiff
    res = taskBufferIF.querySQLS(sqlUDO,varMap)

    # get input datasets
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':type'] = 'input'
    sqlGI  = 'SELECT datasetID,datasetName,masterID FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlGI += 'WHERE jediTaskID=:jediTaskID AND type=:type '
    res = taskBufferIF.querySQLS(sqlGI,varMap)
    inputDatasets = {}
    masterID = None
    for datasetID,datasetName,tmpMasterID in res[1]:
        inputDatasets[datasetID] = datasetName
        if tmpMasterID == None:
            masterID = datasetID

    # check if datasets are still available
    lostDatasets = {}

    # sql to update input file status
    sqlUFI  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
    sqlUFI += 'SET status=:newStatus,attemptNr=attemptNr+1 '
    sqlUFI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus '
    # sql to get affected inputs
    sqlAI  = 'SELECT fileID,datasetID,lfn,outPandaID FROM {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
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
        for fileID,datasetID,lfn,outPandaID in res[1]:
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
            resUF = taskBufferIF.querySQLS(sqlUFI,varMap)
            nRow = resUF[1]
            if nRow > 0:
                datasetCountMap[datasetID] += 1

    # update dataset statistics
    sqlUDI  = 'UPDATE {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
    sqlUDI += 'SET nFilesUsed=nFilesUsed-:nDiff,nFilesFinished=nFilesFinished-:nDiff '
    sqlUDI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
    for datasetID,nDiff in datasetCountMap.iteritems():
        if nDiff > 0:
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':datasetID'] = datasetID
            varMap[':nDiff'] = nDiff
            res = taskBufferIF.querySQLS(sqlUDI,varMap)
            print "  reset {0} files in {1}".format(nDiff, inputDatasets[datasetID])
            
    # update task status
    sqlUT  = 'UPDATE {0}.JEDI_Tasks SET status=:newStatus WHERE jediTaskID=:jediTaskID '.format(jedi_config.db.schemaJEDI)
    varMap = {}
    varMap[':jediTaskID'] = jediTaskID
    varMap[':newStatus'] = 'finished'
    res = taskBufferIF.querySQLS(sqlUT,varMap)
    print "  retry jediTaskID={0}".format(jediTaskID)
    print " ", Client.retryTask(jediTaskID)


import sys
line = sys.argv[1]
datasetName,lostFiles = line.split(':')
lostFiles = set(lostFiles.split(','))

#resetStatusForLostFileRecovery(datasetName,lostFiles)
