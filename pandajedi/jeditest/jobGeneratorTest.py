import sys
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import JobGenerator

parent_conn, child_conn = multiprocessing.Pipe()

try:
    testVO = sys.argv[1]
except:
    testVO = 'atlas'

try:
    testTaskType = sys.argv[2]
except:
    testTaskType = 'test'

try:
    testClouds = sys.argv[3].split(',')
except:
    testClouds = [None]

print testVO,testTaskType,testClouds    

gen = multiprocessing.Process(target=JobGenerator.launcher,
                              args=(child_conn,tbIF,ddmIF,testVO,testTaskType,testClouds,
                                    False,False))
gen.start()
