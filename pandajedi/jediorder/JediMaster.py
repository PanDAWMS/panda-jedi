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
from pandajedi.jedicore.ThreadUtils import ZombiCleaner

from pandajedi.jediconfig import jedi_config

# the master class of JEDI which runs the main process
class JediMaster:

    # constrictor
    def __init__(self):
        pass



    # spawn a knight to have own file descriptors
    def launcher(self,moduleName,*args,**kwargs):
        # import module
        mod = __import__(moduleName)
        for subModuleName in moduleName.split('.')[1:]:
            mod = getattr(mod,subModuleName)
        # launch
        timeNow = datetime.datetime.utcnow()
        print "{0} {1}: INFO    start {2} with pid={3}".format(str(timeNow),
                                                               moduleName,
                                                               'launcher',
                                                               os.getpid())
        apply(mod.launcher,args,kwargs)    
        


    # convert config parameters
    def convParams(self,itemStr):
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
                except:
                    newItems.append(item)
        return newItems


    
    # main loop
    def start(self):
        # start zombi cleaner
        ZombiCleaner().start()
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
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskRefiner',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel))
                proc.start()
                knightList.append(proc)
        # setup TaskBrokerage
        for itemStr in jedi_config.taskbroker.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskBroker',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel))
                proc.start()
                knightList.append(proc)
        # setup ContentsFeeder
        for iproc in range(jedi_config.confeeder.nProc):
            parent_conn, child_conn = multiprocessing.Pipe()
            proc = multiprocessing.Process(target=self.launcher,
                                           args=('pandajedi.jediorder.ContentsFeeder',
                                                 child_conn,taskBufferIF,ddmIF))
            proc.start()
            knightList.append(proc)
        # setup JobGenerator
        for itemStr in jedi_config.jobgen.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            cloud  = items[3]
            if not isinstance(cloud,list):
                cloud = [cloud]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.JobGenerator',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel,cloud,False,False))
                proc.start()
                knightList.append(proc)
        # setup PostProcessor
        for itemStr in jedi_config.postprocessor.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.PostProcessor',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel))
                proc.start()
                knightList.append(proc)
        # setup TaskCommando
        for itemStr in jedi_config.tcommando.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.TaskCommando',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel))
                proc.start()
                knightList.append(proc)
        # setup WatchDog
        for itemStr in jedi_config.watchdog.procConfig.split(';'):
            items = self.convParams(itemStr)
            vo     = items[0]
            plabel = items[1]
            nProc  = items[2]
            for iproc in range(nProc):
                parent_conn, child_conn = multiprocessing.Pipe()
                proc = multiprocessing.Process(target=self.launcher,
                                               args=('pandajedi.jediorder.WatchDog',
                                                     child_conn,taskBufferIF,ddmIF,
                                                     vo,plabel))
                proc.start()
                knightList.append(proc)
        # join
        for knight in knightList:    
            knight.join()



# kill whole process
def catch_sig(sig,frame):
    # kill
    os.killpg(os.getpgrp(),signal.SIGKILL)



# main
if __name__ == "__main__":
    # parse option
    parser = optparse.OptionParser()
    parser.add_option('--pid',action='store',dest='pid',default=None,
                      help='pid filename')
    options,args = parser.parse_args()
    uid = pwd.getpwnam(jedi_config.master.uname).pw_uid
    gid = grp.getgrnam(jedi_config.master.gname).gr_gid
    # make daemon context
    dc = daemon.DaemonContext(stdout=sys.stdout,
                              stderr=sys.stderr,
                              uid=uid,
                              gid=gid)
    with dc:
        # record PID
        pidFile = open(options.pid,'w')
        pidFile.write('{0}'.format(os.getpid()))
        pidFile.close()
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM,catch_sig)
        # start master
        master = JediMaster()
        master.start()
