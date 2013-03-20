import sys

from pandaserver import taskbuffer
import taskbuffer.OraDBProxy
from WorkQueueMapper import WorkQueueMapper

from taskbuffer.OraDBProxy import _logger

class DBProxy(taskbuffer.OraDBProxy.DBProxy):

    # constructor
    def __init__(self,useOtherError=False):
        taskbuffer.OraDBProxy.DBProxy.__init__(self,useOtherError)
        # attributes for JEDI
        # 
        # list of work queues
        self.workQueueMap = WorkQueueMapper()
        # update time for work queue map
        self.updateTimeForWorkQueue = None


    # connect to DB (just for INTR)
    def connect(self,dbhost=None,dbpasswd=None,
                dbuser=None,dbname=None,
                dbtimeout=None,reconnect=False):
        return taskbuffer.OraDBProxy.DBProxy.connect(self,dbhost=dbhost,dbpasswd=dbpasswd,
                                                     dbuser='ATLAS_PANDA',dbname=dbname,
                                                     dbtimeout=dbtimeout,reconnect=reconnect)

    # refresh work queue map
    def refreshWrokQueueMap(self):
        # avoid frequent lookup
        if self.updateTimeForWorkQueue != None and \
               (datetime.datetime.utcnow()-self.self.updateTimeForWorkQueue) < datetime.timedelta(hours=3):
            return
        comment = ' /* JediDBProxy.refreshWrokQueueMap */'
        _logger.debug('%s start' % comment) 
        # SQL
        sql = self.workQueueMap.getSqlQuery()
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make map
            self.workQueueMap.makeMap(res)
            return True
        except:
            # error
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("%s : %s %s" % (comment,errtype,errvalue))
            return False
                                            
                            
