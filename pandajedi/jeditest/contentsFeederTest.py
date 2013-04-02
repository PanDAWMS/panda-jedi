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
                                                args=(child_conn,tbIF,ddmIF))
contentsFeeder.daemon = True
contentsFeeder.start()

"""
from pandajedi.jedicore.JediTaskSpec import JediTaskSpec

task = JediTaskSpec()
task.taskID = 1
task.taskName = 'aaa'
task.status = 'defined'
task.userName = 'pandasrv1'
task.vo = 'atlas'
tbIF.insertTaskJEDI(task) 

from pandajedi.jedicore.JediDatasetSpec import JediDatasetSpec
ds = JediDatasetSpec()
ds.taskID = 1
ds.datasetName = 'data12_8TeV.00214651.physics_Egamma.merge.AOD.f489_m1261'
ds.type = 'input'
ds.vo = 'atlas'
ds.status = 'defined'
tbIF.insertDatasetJEDI(ds)

"""