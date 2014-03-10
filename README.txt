Release Note

* 3/10/2014
  * added a mechanism for task chain
  * fixed JEDI email notification to show process and dataset summary

* 3/7/2014
  * fixed wrong errorDiaglog="No build jobs succeeded" for group anal

* 3/4/2014
  * added protection to TR against errors happen before impl or taskSpec is generated
  * changed finished/partial to done/finished

* 3/3/2014
  * changed watchdog to allow vo/label specific acction
  * enabled FAX

* 2/20/2014
  * changed ATLAS job brokerage to allow jobs go to brokeroff when preassigned 
  * removed disk size check for directIO sites from ATLAS job brokerage
  * enabled rucio endpoint

* 2/17/2014
  * fixed disk threshold for anal brokerage
  * fixed for transHome=None

* 2/13/2014
  * changed for new columns in t_task

* 2/11/2014
  * ignoring panda internal datasets
  * added protection for unknown dataset
  * support for wild-card in input dataset/container names

* 2/7/2014
  * fixed getScoutData to calculate data per MB

* 2/6/2014
  * fixed AtlasJB to use LOCALGROUPDISK

* 2/4/2014
  * adding attemptNr to output/log LFN
  * fixed for log+ObjectStore

* 2/3/2014
  * implemented event service functions
  * fixed ATLAS pp to delete transient or empty datasets
  * fixed ATLAS job brokerge for nightlies 

* 1/29/2014
  * added protection against RUCIOTEST
  * making new lib when previous attempt failed at the same site

* 1/22/2014
  * added pfnList
  * fixed disableAutoRetry to disable JEDI retry too
  * enabled recordRetryHistory_JEDI

* 1/19/2014
  * added disableAutoRetry

* 1/17/2014
  * added support for loadXML
  * using Job_Retry_History

* 1/15/2014
  * added safety merging to the output offset
  * fixed to set scope for lib

* 1/14/2014
  * fixed getScoutData to use lib size as work dir size
  * fixed InputChunk to use the output offset

* 1/13/2014
  * not chaning task status by merging
  * closing libDS
  * fixed to send email when no scouts succeeded
  * fixed to use unique name for lib.tgz
 
* 1/9/2014
  * fixed JG for cache

* 12/20/2013
  * fixed for multiple input streams

* 12/19/2013
  * added --noEmail
  * incexec with dsForXYZ

* 12/17/2013
  * changed to work with T_TASK
  
* 12/13/2013
  * added include/exclude in CF
  * set containerName when expand

* 12/12/2013
  * fixed for input container
  * fixed for DBR caching
  * added protection against task duplication
  * fixed for retry

* 12/11/2013
  * fixed AnalRefiner for DBR
  * fixed AtlasAnalJobBroker for data locality
  * fixed makeJobParameters for merging

* 12/10/2013
  * added pandamon URLs to AtlasAnalPostProcessor
  * fixed AtlasAnalJobBrokerage for remote access
  * implemented TaskSetupper for ATLAS analysis

* 12/2/2013 
  * set task status to broken when no scout jobs succeeded
  * fixed retry for scouting

* 12/1/2013 
  * increased the limit on the number of files 

* 11/25/2013 
  * refactored PostProcessor
  * added email notification

* 11/22/2013 
  * removed direct LFC dependence from AtlasDDMClient

* 11/21/2013 
  * setting transferType and sourceSite when remote access is used
  * implemented eventPicking and GRL

* 11/18/2013
  * changed getBestNNetworkSites_JEDI

* 11/15/2013
  * implemented task retry and incexec
  * added a protection to CF for too many input

* 11/9/2013
  * fixed getBestNNetworkSites_JEDI for table merging

* 11/7/2013
  * implemented merging

* 10/29/2013
  * added containerExpansion

* 10/20/2013
  * added nEventsPerRange
  * fixed CF to take offset for input into account

* 10/11/2013
  * added check if DBR is well replicated
  * fixed for /E
  * set jobsetID
  * added LATEST DBR lookup

* 10/10/2013
  * improved JG for pathena

* 10/4/2013
  * implemented getSatelliteSites
 
* 9/26/2013
  * added preprocessing function

* 9/9/2013
  * added analysis functions
  * tagged 0.0.1

* 9/4/2013
  * fixed first event to start from 1
  * fixed random seed

* 8/31/2013
  * fixed prepareTask

* 8/29/2013
  * fixed Watchdoc
  * uploading logs when setupper is failed

* 8/28/2013
  * added support for log merge

* 8/19/2013
  * added timeout for pending

* 8/12/2013
  * fixed CF for nFilesPerJob + scouting 

* 8/9/2013
  * fixed splitter to respect nFilesPerJob even in scouting 

* 8/8/2013
  * added a capability to make build jobs

* 7/31/2013
  * added fullSimulation for job splitter 
  * removed defualt walltime in AtlasProdTaskRefiner
  * improved AtlasJobBroker to upload log snippet
  * changed AtlasJobBroker to take max/minmemory and mintime into account

* 7/29/2013
  * added an error message to CF when files are missing
  * changed FR to take oldAccompanyDatasetNames into account

* 7/22/2013
  * added TaskSetupper

* 7/19/2013
  * added TG for FileRecovery

* 7/16/2013
  * fixed JG for FileRecovery

* 7/11/2013
  * fixed getScoutJobData

* 7/10/2013
  * added FileRecovery

* 7/3/2013
  * alpha version 

* 6/28/2013
  * added missing file remover to AtlasProdTaskBroker 

* 6/25/2013
  * fixed JG to take maxNumJob into account
  * added reniceJEDI

* 6/21/2013
  * fixed JobThrottler for maxNumJob and minPriority

* 6/20/2013
  * added jediTaskID to the WHERE clause for all UPDATE

* 6/19/2013
  * improved CF to directly avalanche when skipScout=True
  * improved JG to reset unused files
  * improved some queries to use AUX table
  * fixed AtlasDDMClient to ignore duplicated files with different attempt number
  * added support for non-input tasks

* 6/14/2013
  * fixed to support secondary datasets which has non-integer ratio to master 

* 6/11/2013
  * fixed WorkQueueMapper

* 6/6/2013
  * added ZombiCleaner

* 6/3/2013
  * changed schema names configurable
  * fixed CF for broken datasets

* 6/1/2013
  * added TaskCommando

* 5/27/2013
  * added TaskBroker

* 5/22/2013
  * renamed taskID to jediTaskID

* 5/10/2013
  * first version
