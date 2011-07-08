#!/usr/bin/env python
## -*- coding: utf-8 -*-

import os,sys

SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
print SOPHIE_BASE
sys.path.append(SOPHIE_BASE)


from sqlalchemy import create_engine,func,and_,asc
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean
from datetime import timedelta,datetime,date
from sophie.lib.connectors.vurve.db.merchant_db_orm import StoreAccount
from sophie.lib.connectors.vurve.db.pixel_db_orm import Click
from sophie.lib.bigbrother.clicksession_db_orm import  ClickSession, SessionMkg, Histogram, StoreMap
from sophie.lib.connectors.vurve.db.merchant_db import MerchantDbDao
import sophie.lib.bigbrother.clicksession_db as clickdb
import sophie.lib.bigbrother.proxy_db as proxdb
import logging
import yaml
import pickle
#Load the logger 
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)


#Load the configurations for the databases
config = yaml.load(open('conifg.yml'))
pklConfig = config["ordercorrelator"]

#Load the path of the open session pickle
_lastLoadedPath = pklConfig["laststoreloadedpath"]
_timedelta_search = timedelta(hours=3)

_store_map = {}
#Will contain a list of stores that have at least a session
_instrumented_stores = set()

def LoadInstrumentedStores():
    qry = clickdb.QueryStoresWithSessions()
    for st in qry:
        _instrumented_stores.add(st[0])
    

#This function makes sure that the store map is up to date
def UpdateMapper():
   #make sure the database is up to date
   clickdb.CreateTables()
   log.debug("Loading stores created till today")
   
   if os.path.exists(_lastLoadedPath):
       log.debug("Pickle with last loaded store map date exists at {0}".format(_lastLoadedPath))
       (latestCreatedStore) = pickle.load(open(_lastLoadedPath,'r'))
   else:
       log.debug("No pickle date of last store map found at {0}. Will load all stores created after {1}".\
                 format(_lastLoadedPath,proxdb._firstStoreDate))
       latestCreatedStore = proxdb._firstStoreDate
   log.debug("Latest created store {0}".format(latestCreatedStore))
   stores = []
   storeQry=proxdb.QueryAllStoreIdPIDPAirsCreatedAfterDate(latestCreatedStore)
   newLatestCreated = latestCreatedStore
   #Read All stores
   for s in storeQry:
       sm = StoreMap()
       sm.store_id = s[0]
       sm.public_id = s[1]
       if newLatestCreated < s[2]:
           newLatestCreated = s[2]
       stores.append(sm)

   clickdb.PersistStoreMap(stores)
   #Save the new date
   pickle.dump(newLatestCreated,open(_lastLoadedPath,'wb'))

   mapResultSet=clickdb.GetStoreMap()
   for m in mapResultSet:
       _store_map[m.store_id]=m.public_id
 
def CorrelateOrders(datetime_start,datetime_end):
    #load orders in the date range and look for sessions that correspond
    
    orderQuery = MerchantDbDao.get_merchant_dao().get_orders_in_date_range(datetime_start,datetime_end)
    log.debug("Extracted {0} orders".format(orderQuery.count())) 
    sessionQuery = clickdb.QuerySessions(datetime_start-_timedelta_search,datetime_end)
    log.debug("Extracted {0} sessions".format(sessionQuery.count()))
    

    instrumentedOrders = filter(lambda x: _store_map.has_key(x.store_id)\
                                and x.referring_site != ''\
                                and x.referring_site is not None \
                                and (_store_map[x.store_id] in _instrumented_stores), orderQuery)

    #Sort the session queries by ending time
    sorted_sessions = sorted(sessionQuery, key=lambda sq:sq.time_start)

    for order in instrumentedOrders:
        #Get the public id
        pubid = _store_map[order.store_id]
        candidates = []
        for x in sorted_sessions:
            if x.time_start>order.created_at:
                break
            if x.public_id == pubid and x.ref is not None and x.ref.startswith(order.referring_site) and x.time_start<order.created_at:
                candidates.append[x]

        #If there was only one candidate asume it it correct
        log.debug("Processed order {0}".format(order.order_id))
        if candidates.count(candidates) == 1:
            sc.order_id = order.order_id
            print "Match Found: \n \t Order Created At {0} \n \t Session Started At {1} \n \t Ref {2} \n \t Order Ref Stie: {3}".\
                                   format(order.created_at,sc.time_start,sc.ref,order.referring_site)


if __name__=="__main__":
    print "Loading all the entries"
    UpdateMapper()
    LoadInstrumentedStores()
    start = datetime(year=2011,month=4,day=20)
    CorrelateOrders(start,start+timedelta(days=10))
