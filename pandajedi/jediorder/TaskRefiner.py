import sys
import time
import datetime

from pandajedi.jedicore.ThreadUtils import ListWithLock,ThreadPool,WorkerThread
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedirefine import RefinerUtils
from JediKnight import JediKnight

from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
from pandajedi.jediconfig import jedi_config


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# worker class to refine TASK_PARAM to fill JEDI tables 
class TaskRefiner (JediKnight,FactoryBase):

    # constructor
    def __init__(self,commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        JediKnight.__init__(self,commuChannel,taskBufferIF,ddmIF,logger)
        FactoryBase.__init__(self,self.vos,self.prodSourceLabels,logger,
                             jedi_config.taskrefine.modConfig)


    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self,self.taskBufferIF,self.ddmIF)
        # go into main loop
        while True:
            startTime = datetime.datetime.utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug('start')
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of tasks to refine
                        tmpList = self.taskBufferIF.getTasksToRefine_JEDI(vo,prodSourceLabel)
                        if tmpList == None:
                            # failed
                            tmpLog.error('failed to get the list of tasks to refine')
                        else:
                            tmpLog.debug('got {0} tasks'.format(len(tmpList)))
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # get work queue mapper
                            workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                            # make workers
                            nWorker = jedi_config.taskrefine.nWorkers
                            for iWorker in range(nWorker):
                                thr = TaskRefinerThread(taskList,threadPool,
                                                        self.taskBufferIF,
                                                        self,workQueueMapper)
                                thr.start()
                            # join
                            threadPool.join()
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed in {0}.start() with {1} {2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
            # sleep if needed
            loopCycle = jedi_config.taskrefine.loopCycle
            timeDelta = datetime.datetime.utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)



# thread for real worker
class TaskRefinerThread (WorkerThread):

    # constructor
    def __init__(self,taskList,threadPool,taskbufferIF,implFactory,workQueueMapper):
        # initialize woker with no semaphore
        WorkerThread.__init__(self,None,threadPool,logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.implFactory = implFactory
        self.workQueueMapper = workQueueMapper


    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug('{0} terminating since no more items'.format(self.__class__.__name__))
                    return
                # loop over all tasks
                for jediTaskID in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger,'jediTaskID={0}'.format(jediTaskID))
                    tmpLog.info('start')
                    tmpStat = Interaction.SC_SUCCEEDED
                    # convert to map
                    try:
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('conversion to map from json failed with {0}:{1}'.format(errtype.__name__,errvalue))
                        tmpStat = Interaction.SC_FAILED
                    # get impl
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('getting Impl')
                        try:
                            # get VO and sourceLabel                            
                            vo = taskParamMap['vo']
                            prodSourceLabel = taskParamMap['prodSourceLabel']
                            # get impl
                            impl = self.implFactory.getImpl(vo,prodSourceLabel)
                            if impl == None:
                                # task refiner is undefined
                                tmpLog.error('task refiner is undefined for vo={0} sourceLabel={1}'.format(vo,prodSourceLabel))
                                tmpStat = Interaction.SC_FAILED
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('getImpl failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                    # extract common parameters
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('extracting common')                    
                        try:
                            # initalize impl
                            impl.initializeRefiner(tmpLog)
                            # extarct common parameters
                            impl.extractCommon(jediTaskID,taskParamMap,self.workQueueMapper)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('extractCommon failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                    # refine
                    if tmpStat == Interaction.SC_SUCCEEDED:
                        tmpLog.info('refining with {0}'.format(impl.__class__.__name__))
                        try:
                            tmpStat = impl.doRefine(impl.taskSpec.jediTaskID,impl.taskSpec.taskType,taskParamMap)
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('doRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
                            tmpStat = Interaction.SC_FAILED
                    # register
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error('failed to refine the task')
                        # FIXME
                        # update task
                    else:
                        tmpLog.info('registering')                    
                        # fill JEDI tables
                        try:
                            tmpStat = self.taskBufferIF.registerTaskInOneShot_JEDI(jediTaskID,impl.taskSpec,
                                                                                   impl.inMasterDatasetSpec,
                                                                                   impl.inSecDatasetSpecList,
                                                                                   impl.outDatasetSpecList,
                                                                                   impl.outputTemplateMap,
                                                                                   impl.jobParamsTemplate)
                            if not tmpStat:
                                tmpLog.error('failed to register the task to JEDI')
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            tmpLog.error('failed to register the task to JEDI with {0}:{1}'.format(errtype.__name__,errvalue))
                        else:
                            tmpLog.info('done')
            except:
                errtype,errvalue = sys.exc_info()[:2]
                logger.error('{0} failed in runImpl() with {1}:{2}'.format(self.__class__.__name__,errtype.__name__,errvalue))
        


########## lauch 
                
def launcher(commuChannel,taskBufferIF,ddmIF,vos=None,prodSourceLabels=None):
    p = TaskRefiner(commuChannel,taskBufferIF,ddmIF,vos,prodSourceLabels)
    p.start()
