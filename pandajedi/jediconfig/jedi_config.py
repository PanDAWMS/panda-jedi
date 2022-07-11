import os
import re
import sys
import json

from six import iteritems

from liveconfigparser.LiveConfigParser import LiveConfigParser, expand_values

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_jedi.cfg')

# dummy section class
class _SectionClass:
    pass

# load configmap
config_map_data = {}
if 'PANDA_HOME' in os.environ:
    config_map_name = 'panda_jedi_config.json'
    config_map_path = os.path.join(os.environ['PANDA_HOME'], 'etc/config_json', config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            config_map_data = json.load(f)

# loop over all sections
for tmpSection in tmpConf.sections():
    # read section
    tmpDict = getattr(tmpConf, tmpSection)
    # load configmap
    if tmpSection in config_map_data:
        tmpDict.update(config_map_data[tmpSection])
    # make section class
    tmpSelf = _SectionClass()
    # update module dict
    sys.modules[ __name__ ].__dict__[tmpSection] = tmpSelf
    # expand all values
    expand_values(tmpSelf, tmpDict)
