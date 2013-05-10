from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import WatchDog

parent_conn, child_conn = multiprocessing.Pipe()

watchDog = multiprocessing.Process(target=WatchDog.launcher,
                                   args=(child_conn,tbIF,ddmIF))
watchDog.start()
