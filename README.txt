Release Note

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
