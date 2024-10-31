import datetime
import os
import re
import socket
import sys
import traceback

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore.DataCarousel import (
    DataCarouselInterface,
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
)
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


class AtlasDataCarouselWatchDog(WatchDogBase):
    """
    Data Carousel watchdog for ATLAS
    """

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        self.vo = "atlas"
        self.ddmIF = ddmIF.getInterface(self.vo)
        self.data_carousel_interface = DataCarouselInterface(taskBufferIF, ddmIF)

    def doStageDCRequests(self):
        """
        Action to get queued DC requests and start staging
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doStageDCRequests")
        tmpLog.debug("start")
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel="default",
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasDataCarousDog.doStageDCReq",
                pid=self.pid,
                timeLimit=5,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            tmpLog.debug("got lock")
            # get DC requests to stage
            dc_req_specs = self.data_carousel_interface.get_requests_to_stage()
            # stage the requests
            for dc_req_spec in dc_req_specs:
                self.data_carousel_interface.stage_request(dc_req_spec)
                tmpLog.debug(f"stage request_id={dc_req_spec.request_id} dataset={dc_req_spec.dataset}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doKeepRulesAlive(self):
        """
        Action to keep DDM staging rules alive when tasks running
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doKeepRulesAlive")
        tmpLog.debug("start")
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel="default",
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasDataCarousDog.doKeepRulesAlive",
                pid=self.pid,
                timeLimit=60,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            tmpLog.debug("got lock")
            # keep alive rules
            self.data_carousel_interface.stage_request.keep_alive_ddm_rules()
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doCleanDCRequests(self):
        """
        Action to clean up old DC requests in DB table
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doCleanDCRequests")
        tmpLog.debug("start")
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel="default",
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasDataCarousDog.doCleanDCReq",
                pid=self.pid,
                timeLimit=1440,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            tmpLog.debug("got lock")
            # TODO: to be implemented
            pass
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # clean up old DC requests
            self.doCleanDCRequests()
            # keep staging rules alive
            self.doKeepRulesAlive()
            # stage queued requests
            self.doStageDCRequests()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
