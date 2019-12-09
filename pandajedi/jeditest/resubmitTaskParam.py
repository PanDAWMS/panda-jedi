import os
import re
import sys
import time
import datetime
import threading

from pandajedi.jediconfig import jedi_config

from config import panda_config

from taskbuffer.Initializer import initializer
from pandajedi.jedicore import JediTaskBuffer
from pandalogger.PandaLogger import PandaLogger

jediTaskID = sys.argv[1]

# initialize cx_Oracle using dummy connection
initializer.init()


taskBuffer= JediTaskBuffer.JediTaskBuffer(None)
proxy = taskBuffer.proxyPool.getProxy()

s,o = proxy.getClobObj('select task_param from atlas_deft.deft_task where task_id=:task_id',{':task_id':jediTaskID})

taskParamStr = o[0][0]

proxy.insertTaskParams_JEDI(None,taskParamStr)
