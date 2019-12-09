import sys
import datetime
import multiprocessing

from . import JediCoreUtils

# wrapper for multiprocessing.Process
class ProcessWrapper(multiprocessing.Process):

    # constructor
    def __init__(self,target,args):
        multiprocessing.Process.__init__(self,target=self.wrappedMain)
        self.target = target
        self.args = args


    # main
    def wrappedMain(self):
        while True:
            proc = multiprocessing.Process(target=self.target,
                                           args=self.args)
            proc.start()
            pid = proc.pid
            while True:
                try:
                    proc.join(20)
                    if not JediCoreUtils.checkProcess(pid):
                        timeNow = datetime.datetime.utcnow()
                        print("{0} {1}: INFO    pid={2} not exist".format(str(timeNow),
                                                                          self.__class__.__name__,
                                                                          pid))
                        break
                except Exception:
                    timeNow = datetime.datetime.utcnow()
                    errType,errValue = sys.exc_info()[:2]
                    print("{0} {1}: INFO    failed to check pid={2} with {3} {4}".format(str(timeNow),
                                                                                         self.__class__.__name__,
                                                                                         pid,errType,errValue))
