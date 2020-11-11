import os
import sys
import re
import socket
import traceback

from .WatchDogBase import WatchDogBase
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# data locality updater for ATLAS
class AtlasDataLocalityUpdaterWatchDog(WatchDogBase):

    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        WatchDogBase.__init__(self, ddmIF, taskBufferIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        self.vo = 'atlas'

    # get list-with-lock of datasets to update
    def get_datasets_list(self):
        datasets_list = self.taskBufferIF.get_tasks_inputdatasets_JEDI(self.vo)
        datasets_list = ListWithLock(datasets_list)
        # return
        return datasets_list

    # update data locality records to DB table
    def doUpdateDataLocality(self):
        try:
            tmpLog = MsgWrapper(logger, ' #ATM #KV doUpdateDataLocality')
            tmpLog.debug('start')
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(  vo=self.vo, prodSourceLabel=self.prodSourceLabel,
                                                            cloud=None, workqueue_id=None, resource_name=None,
                                                            component='AtlasDataLocalityUpdaterWatchDog.doUpdateDataLocality',
                                                            pid=self.pid, timeLimit=720)
            if not got_lock:
                tmpLog.debug('locked by another process. Skipped')
                return
            # get list of datasets
            datasets_list = self.get_datasets_list()
            # make thread pool
            thread_pool = ThreadPool()
            # make workers
            n_workers = 4
            for _ in range(n_workers):
                thr = ContentsFeederThread( taskDsList=datasets_list,
                                            threadPool=thread_pool,
                                            taskbufferIF=self.taskBufferIF,
                                            ddmIF=self.ddmIF,
                                            pid=self.pid,
                                            loggerObj=tmpLog)
                thr.start()
            # join
            thread_pool.join()
            # done
            tmpLog.debug('done')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1} {2}'.format(errtype, errvalue, traceback.format_exc()))

    # clean up old data locality records in DB table
    def doCleanDataLocality(self):
        try:
            tmpLog = MsgWrapper(logger, ' #ATM #KV doUpdateDataLocality')
            tmpLog.debug('start')
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(  vo=self.vo, prodSourceLabel=self.prodSourceLabel,
                                                            cloud=None, workqueue_id=None, resource_name=None,
                                                            component='AtlasDataLocalityUpdaterWatchDog.doCleanDataLocality',
                                                            pid=self.pid, timeLimit=1440)
            if not got_lock:
                tmpLog.debug('locked by another process. Skipped')
                return
            # lifetime of records
            record_lifetime_hours = 72
            # run
            now_timestamp = datetime.datetime.utcnow()
            before_timestamp = now_timestamp - datetime.timedelta(hours=record_lifetime_hours)
            n_rows = self.taskBufferIF.deleteOutdatedDatasetLocality_JEDI(before_timestamp)
            tmpLog.info('cleaned up {0} records'.format(n_rows))
            # done
            tmpLog.debug('done')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1} {2}'.format(errtype, errvalue, traceback.format_exc()))

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # clean up data locality
            self.doCleanDataLocality()
            # update data locality
            self.doUpdateDataLocality()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error('failed with {0} {1}'.format(errtype, errvalue))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED


# thread for data locality update
class DataLocalityUpdaterThread(WorkerThread):

    # constructor
    def __init__(self, taskDsList, threadPool, taskbufferIF, ddmIF, pid, loggerObj):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, loggerObj)
        # attributres
        self.taskDsList = taskDsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = 'datalocalityupdate'
        self.pid = pid
        self.logger = loggerObj

    # main
    def runImpl(self):
        while True:
            try:
                # get part of datasets
                nDatasets = 5
                taskDsList = self.taskDsList.get(nDatasets)
                if len(taskDsList) == 0:
                    # no more datasets, quit
                    self.logger.debug('%s terminating since no more items' % self.__class__.__name__)
                    return
                # loop over these datasets
                for item in taskDsList:
                    if item is None:
                        continue
                    jediTaskID, datasetID, datasetName = item
                    dataset_replicas_map = self.ddmIF.listDatasetReplicas(datasetName)
                    for tmpRSE, tmpList in dataset_replicas_map.items():
                        tmpStatistics = tmpList[-1]
                        # exclude unknown
                        if tmpStatistics['found'] is None:
                            continue
                        # update dataset locality table
                        self.taskBufferIF.updateDatasetLocality_JEDI(   jedi_taskid=jediTaskID,
                                                                        datasetid=datasetID,
                                                                        rse=tmpRSE)
            except Exception as e:
                self.logger.error('{0} failed in runImpl() with {1}: {2}'.format(self.__class__.__name__, str(e),
                                                                            traceback.format_exc()))
