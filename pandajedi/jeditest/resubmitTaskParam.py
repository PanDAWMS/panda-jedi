import os
import re
import sys
import time
import datetime
import commands
import threading

from pandajedi.jediconfig import jedi_config

from config import panda_config

jediTaskID = sys.argv[1]

# initialize cx_Oracle using dummy connection
from taskbuffer.Initializer import initializer
initializer.init()

from pandajedi.jedicore import JediTaskBuffer
from pandalogger.PandaLogger import PandaLogger

taskBuffer= JediTaskBuffer.JediTaskBuffer(None)
proxy = taskBuffer.proxyPool.getProxy()

s,o = proxy.getClobObj('select task_param from atlas_deft.deft_task where task_id=:task_id',{':task_id':jediTaskID})

taskParamStr = o[0][0]

proxy.insertTaskParams_JEDI(None,taskParamStr)
