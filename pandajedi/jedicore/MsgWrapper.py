class MsgWrapper:
    def __init__(self,logger,token=None):
        self.logger = logger
        # use timestamp as token if undefined
        if token == None:
            import datetime
            self.token = datetime.datetime.utcnow().isoformat('/')
        else:
            self.token = token

    def info(self,msg):
        self.logger.info(self.token + ' ' + msg)

    def debug(self,msg):
        self.logger.debug(self.token + ' ' + msg)

    def error(self,msg):
        self.logger.error(self.token + ' ' + msg)

    def warning(self,msg):
        self.logger.warning(self.token + ' ' + msg)
