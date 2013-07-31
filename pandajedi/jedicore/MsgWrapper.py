import datetime
from pandaserver.userinterface import Client


class MsgWrapper:

    def __init__(self,logger,token=None,lineLimit=500):
        self.logger = logger
        # use timestamp as token if undefined
        if token == None:
            self.token = "<{0}>".format(datetime.datetime.utcnow().isoformat('/'))
        else:
            self.token = token
        # message buffer
        self.msgBuffer = []
        self.lineLimit = lineLimit


    def keepMsg(self,msg):
        # keep max message depth
        if len(self.msgBuffer) > self.lineLimit:
            self.msgBuffer.pop(0)
        timeNow = datetime.datetime.utcnow()
        self.msgBuffer.append('{0} : {1}'.format(timeNow.isoformat(' '),msg))


    def info(self,msg):
        msg = str(msg)
        self.logger.info(self.token + ' ' + msg)
        self.keepMsg(msg)


    def debug(self,msg):
        msg = str(msg)
        self.logger.debug(self.token + ' ' + msg)
        self.keepMsg(msg)


    def error(self,msg):
        msg = str(msg)
        self.logger.error(self.token + ' ' + msg)
        self.keepMsg(msg)


    def warning(self,msg):
        msg = str(msg)
        self.logger.warning(self.token + ' ' + msg)
        self.keepMsg(msg)


    def dumpToString(self):
        strMsg = ''
        for msg in self.msgBuffer:
            strMsg += msg
            strMsg += "\n"
        return strMsg


    def uploadLog(self,id):
        strMsg = self.dumpToString()
        s,o = Client.uploadLog(strMsg,id)
        if s != 0:
            return "failed to upload log with {0}.".format(s)
        if o.startswith('http'):
            return '<a href="{0}">log</a>'.format(o)
        return o
