import os
import sys
import re
import datetime
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
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        self.vo = 'atlas'
        self.ddmIF = ddmIF.getInterface(self.vo)

    # get list-with-lock of datasets to update
    def get_datasets_list(self):
        datasets_list = self.taskBufferIF.get_tasks_inputdatasets_JEDI(self.vo)
        datasets_list = ListWithLock(datasets_list)
        # return
        return datasets_list

    # update data locality records to DB table
    def doUpdateDataLocality(self):
        tmpLog = MsgWrapper(logger, ' #ATM #KV doUpdateDataLocality')
        tmpLog.debug('start')
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(  vo=self.vo, prodSourceLabel='default',
                                                            cloud=None, workqueue_id=None, resource_name=None,
                                                            component='AtlasDataLocalityUpdaterWatchDog.doUpdateDataLocality',
                                                            pid=self.pid, timeLimit=240)
            if not got_lock:
                tmpLog.debug('locked by another process. Skipped')
                return
            tmpLog.debug('got lock')
            # get list of datasets
            datasets_list = self.get_datasets_list()
            tmpLog.debug('got {0} datasets to update'.format(len(datasets_list)))
            # make thread pool
            thread_pool = ThreadPool()
            # make workers
            n_workers = 4
            for _ in range(n_workers):
                thr = DataLocalityUpdaterThread(taskDsList=datasets_list,
                                                threadPool=thread_pool,
                                                taskbufferIF=self.taskBufferIF,
                                                ddmIF=self.ddmIF,
                                                pid=self.pid,
                                                loggerObj=tmpLog)
                thr.start()
            tmpLog.debug('started {0} updater workers'.format(n_workers))
            # join
            thread_pool.join()
            # done
            tmpLog.debug('done')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error('failed with {0} {1} {2}'.format(errtype, errvalue, traceback.format_exc()))

    # clean up old data locality records in DB table
    def doCleanDataLocality(self):
        tmpLog = MsgWrapper(logger, ' #ATM #KV doCleanDataLocality')
        tmpLog.debug('start')
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(  vo=self.vo, prodSourceLabel='default',
                                                            cloud=None, workqueue_id=None, resource_name=None,
                                                            component='AtlasDataLocalityUpdaterWatchDog.doCleanDataLocality',
                                                            pid=self.pid, timeLimit=1440)
            if not got_lock:
                tmpLog.debug('locked by another process. Skipped')
                return
            tmpLog.debug('got lock')
            # lifetime of records
            record_lifetime_hours = 24
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
                    self.logger.debug('{0} terminating since no more items'.format(self.name))
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
                return
