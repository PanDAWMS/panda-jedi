from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

from pandajedi.jediddm.DDMInterface import DDMInterface

ddmIF = DDMInterface()
ddmIF.setupInterface()

import multiprocessing

from pandajedi.jediorder import ContentsFeeder

parent_conn, child_conn = multiprocessing.Pipe()

contentsFeeder = multiprocessing.Process(target=ContentsFeeder.launcher,
                                                args=(child_conn,tbIF,ddmIF,
                                                      'atlas','test'))
contentsFeeder.start()
