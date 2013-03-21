import re
import sys

from pandaserver.config import panda_config

from liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_jedi.cfg')

# get JEDI section
tmpDict = tmpConf.jedi

# expand all values
tmpSelf = sys.modules[ __name__ ]
for tmpKey,tmpVal in tmpDict.iteritems():
    # convert string to bool/int
    if tmpVal == 'True':
        tmpVal = True
    elif tmpVal == 'False':
        tmpVal = False
    elif re.match('^\d+$',tmpVal):
        tmpVal = int(tmpVal)
    # update dict
    tmpSelf.__dict__[tmpKey] = tmpVal
                                                        
