import sys
import time
import threading
import multiprocessing


# list with lock
class ListWithLock:
    def __init__(self,dataList):
        self.lock = threading.Lock()
        self.dataList  = dataList
        self.dataIndex = 0

    def __contains__(self,item):
        self.lock.acquire()
        ret = self.dataList.__contains__(item)
        self.lock.release()
        return ret

    def append(self,item):
        self.lock.acquire()
        appended = False
        if not item in self.dataList:
            self.dataList.append(item)
            appended = True
        self.lock.release()
        return appended

    def get(self,num):
        self.lock.acquire()
        retList = self.dataList[self.dataIndex:self.dataIndex+num]
        self.dataIndex += num
        self.lock.release()
        return retList

    def stat(self):
        self.lock.acquire()
        total = len(self.dataList)
        nIndx = self.dataIndex
        self.lock.release()
        return total,nIndx



# thread pool
class ThreadPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = []
        
    def add(self,obj):
        self.lock.acquire()
        self.list.append(obj)
        self.lock.release()
        
    def remove(self,obj):
        self.lock.acquire()
        self.list.remove(obj)
        self.lock.release()
        
    def join(self):
        self.lock.acquire()
        thrlist = tuple(self.list)
        self.lock.release()
        for thr in thrlist:
            thr.join()



# thread class working with semaphore and thread pool
class WorkerThread (threading.Thread):

    # constructor
    def __init__(self,workerSemaphore,threadPool,logger):
        threading.Thread.__init__(self)
        self.workerSemaphore = workerSemaphore
        self.threadPool = threadPool
        self.threadPool.add(self)
        self.logger = logger

    # main loop
    def run(self):
        # get slot
        if self.workerSemaphore != None:
            self.workerSemaphore.acquire()
        # execute real work
        try:
            self.runImpl()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            self.logger.error("%s crashed in WorkerThread.run() with %s:%s" % \
                              (self.__class__.__name__,errtype.__name__,errvalue))
        # remove self from thread pool
        self.threadPool.remove(self)
        # release slot
        if self.workerSemaphore != None:
            self.workerSemaphore.release()
            


# thread class to cleanup zombi processes
class ZombiCleaner (threading.Thread):

    # constructor
    def __init__(self,interval=60):
        threading.Thread.__init__(self)
        self.interval = interval


    # main loop
    def run(self):
        while True:
            x = multiprocessing.active_children()
            time.sleep(self.interval)
