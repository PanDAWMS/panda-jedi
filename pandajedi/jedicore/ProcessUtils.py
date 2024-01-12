import datetime
import multiprocessing
import sys

from . import JediCoreUtils


# wrapper for multiprocessing.Process
class ProcessWrapper(multiprocessing.Process):
    # constructor
    def __init__(self, target, args):
        multiprocessing.Process.__init__(self, target=self.wrappedMain)
        self.target = target
        self.args = args

    # main
    def wrappedMain(self):
        while True:
            proc = multiprocessing.Process(target=self.target, args=self.args)
            proc.start()
            pid = proc.pid
            while True:
                try:
                    proc.join(20)
                    if not JediCoreUtils.checkProcess(pid):
                        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                        print(f"{str(timeNow)} {self.__class__.__name__}: INFO    pid={pid} not exist")
                        break
                except Exception:
                    timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    errType, errValue = sys.exc_info()[:2]
                    print(f"{str(timeNow)} {self.__class__.__name__}: INFO    failed to check pid={pid} with {errType} {errValue}")
