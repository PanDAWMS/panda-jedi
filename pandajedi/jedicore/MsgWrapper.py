import datetime

class MsgWrapper:
    def __init__(self,logger,token=None):
        self.logger = logger
        # use timestamp as token if undefined
        if token == None:
            self.token = "<{0}>".format(datetime.datetime.utcnow().isoformat('/'))
        else:
            self.token = token

    def info(self,msg):
        msg = str(msg)
        self.logger.info(self.token + ' ' + msg)

    def debug(self,msg):
        msg = str(msg)
        self.logger.debug(self.token + ' ' + msg)

    def error(self,msg):
        msg = str(msg)
        self.logger.error(self.token + ' ' + msg)

    def warning(self,msg):
        msg = str(msg)
        self.logger.warning(self.token + ' ' + msg)
