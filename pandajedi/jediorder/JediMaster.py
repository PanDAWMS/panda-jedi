import os
import pwd
import grp
import sys
import time
import signal
import daemon
import optparse
import datetime
import multiprocessing

from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.ThreadUtils import ZombieCleaner
from pandajedi.jedicore.ProcessUtils import ProcessWrapper

from pandajedi.jediconfig import jedi_config


# the master class of JEDI which runs the main process
class JediMaster:

    # constrictor
    def __init__(self):
        self.stopEventList = []

    # spawn a knight to have own file descriptors
    def launcher(self, moduleName, *args, **kwargs):
        # import module
        mod = __import__(moduleName)
        for subModuleName in moduleName.split('.')[1:]:
            mod = getattr(mod, subModuleName)
        # launch
        timeNow = datetime.datetime.utcnow()
        print("{0} {1}: INFO    start {2} with pid={3}".format(str(timeNow),
                                                               moduleName,
                                                               'launcher',
                                                               os.getpid()))
        mod.launcher(*args, **kwargs)

    # convert config parameters
    def convParams(self, itemStr):
        items = itemStr.split(':')
        newItems = []
        for item in items:
            if item == '':
                newItems.append(None)
            elif ',' in item:
                newItems.append(item.split(','))
            else:
                try:
                    newItems.append(int(item))
                except Exception:
                    newItems.append(item)
        return newItems

    # main loop
    def start(self):
        # start zombi cleaner
        ZombieCleaner().start()
        # setup DDM I/F
        ddmIF = DDMInterface()
        ddmIF.setupInterface()
        # setup TaskBuffer I/F
        taskBufferIF = JediTaskBufferInterface()
        taskBufferIF.setupInterface()
        # the list of JEDI knights
        knightList = []
        # setup TaskRefiner
        for itemStr in jedi_config.taskrefine.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskRefiner',
                                                     child_conn, taskBufferIF, ddmIF,
                                                     vo, plabel))
                proc.start()
                knightList.append(proc)
        # setup TaskBrokerage
        for itemStr in jedi_config.taskbroker.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskBroker',
                                                     child_conn, taskBufferIF, ddmIF,
                                                     vo, plabel))
                proc.start()
                knightList.append(proc)
        # setup ContentsFeeder
        for itemStr in jedi_config.confeeder.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.ContentsFeeder',
                                                     child_conn, taskBufferIF,
                                                     ddmIF, vo, plabel))
                proc.start()
                knightList.append(proc)
        # setup JobGenerator
        for itemStr in jedi_config.jobgen.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            cloud = items[3]
            try:
                loop_cycle = items[4]
            except IndexError:
                loop_cycle = None

            if not isinstance(cloud, list):
                cloud = [cloud]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = ProcessWrapper(target=self.launcher,
                                      args=('pandajedi.jediorder.JobGenerator', child_conn, taskBufferIF, ddmIF, vo,
                                            plabel, cloud, True, True, loop_cycle))
                proc.start()
                knightList.append(proc)
        # setup PostProcessor
        for itemStr in jedi_config.postprocessor.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.PostProcessor',
                                                     child_conn, taskBufferIF, ddmIF,
                                                     vo, plabel))
                proc.start()
                knightList.append(proc)
        # setup TaskCommando
        for itemStr in jedi_config.tcommando.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskCommando',
                                                     child_conn, taskBufferIF, ddmIF,
                                                     vo, plabel))
                proc.start()
                knightList.append(proc)
        # setup WatchDog
        for itemStr in jedi_config.watchdog.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo = items[0]
            plabel = items[1]
            nProc = items[2]
            subStr = items[3] if len(items) > 3 else None
            period = items[4] if len(items) > 4 else None
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.WatchDog',
                                                     child_conn, taskBufferIF, ddmIF,
                                                     vo, plabel, subStr, period))
                proc.start()
                knightList.append(proc)
        # setup JediMsgProcessor agent (only one system process)
        if hasattr(jedi_config, 'msgprocessor') and hasattr(jedi_config.msgprocessor,
                                                            'configFile') and jedi_config.msgprocessor.configFile:
            stop_event = multiprocessing.Event()
            self.stopEventList.append(stop_event)
            parent_conn, child_conn = multiprocessing.Pipe()
            proc = multiprocessing.Process(target=self.launcher,
                                           args=('pandajedi.jediorder.JediMsgProcessor',
                                                 stop_event))
            proc.start()
            knightList.append(proc)
        # setup JediDaemon agent (only one system process)
        if hasattr(jedi_config, 'daemon') and hasattr(jedi_config.daemon, 'enable') and jedi_config.daemon.enable:
            parent_conn, child_conn = multiprocessing.Pipe()
            proc = multiprocessing.Process(target=self.launcher,
                                           args=('pandajedi.jediorder.JediDaemon',
                                                 taskBufferIF, ddmIF))
            proc.start()
            knightList.append(proc)
        # check initial failures
        time.sleep(5)
        for knight in knightList:
            if not knight.is_alive():
                timeNow = datetime.datetime.utcnow()
                print("{0} {1}: ERROR    pid={2} died in initialization".format(str(timeNow),
                                                                                self.__class__.__name__,
                                                                                knight.pid))
                os.killpg(os.getpgrp(), signal.SIGKILL)
        # join
        for knight in knightList:
            knight.join()

    # graceful stop
    def stop(self):
        for stop_event in self.stopEventList:
            stop_event.set()


# kill whole process
def kill_whole(sig, frame):
    # kill
    os.killpg(os.getpgrp(), signal.SIGKILL)


# main
if __name__ == "__main__":
    # parse option
    parser = optparse.OptionParser()
    parser.add_option('--pid', action='store', dest='pid', default=None,
                      help='pid filename')
    options, args = parser.parse_args()
    uid = None
    gid = None
    if 'PANDA_NO_ROOT' not in os.environ:
        if jedi_config.master.uname:
            uid = pwd.getpwnam(jedi_config.master.uname).pw_uid
        if jedi_config.master.gname:
            gid = grp.getgrnam(jedi_config.master.gname).gr_gid
    timeNow = datetime.datetime.utcnow()
    print("{0} JediMaster: INFO    start".format(str(timeNow)))
    # make daemon context
    dc = daemon.DaemonContext(stdout=sys.stdout,
                              stderr=sys.stderr,
                              uid=uid,
                              gid=gid)
    with dc:
        # record PID
        go_ahead = True
        try:
            if options.pid:  # pid files are no longer necessary in systemd
                pidFile = open(options.pid, 'x')
        except FileExistsError:
            print("{} JediMaster: ERROR    terminated since pid file {} already exists".format(str(timeNow),
                                                                                               options.pid))
            go_ahead = False
        if go_ahead:

            if options.pid:  # pid files are no longer necessary in systemd
                pidFile.write('{0}'.format(os.getpid()))
                pidFile.close()

            # master
            master = JediMaster()

            # set handler
            def catch_sig(sig, frame):
                master.stop()
                time.sleep(3)
                kill_whole(sig, frame)

            signal.signal(signal.SIGINT, catch_sig)
            signal.signal(signal.SIGHUP, catch_sig)
            signal.signal(signal.SIGTERM, catch_sig)
            # start master
            master.start()
