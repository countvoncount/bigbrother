#!/usr/bin/env python
## -*- coding: utf-8 -*-

"""
This file contains the database access layer for the
clickSession database produced from live view.
"""
import os,sys
SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
sys.path.append(SOPHIE_BASE)

from sqlalchemy import create_engine,func,and_,asc
from sqlalchemy.orm import scoped_session,sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean
from sqlalchemy.sql.expression import desc 
import paste,pickle
from datetime import timedelta,datetime,date
from sophie.lib.connectors.vurve.db.merchant_db_orm import StoreAccount
from sophie.lib.connectors.vurve.db.pixel_db_orm import Click
from sophie.lib.bigbrother.clicksession_db_orm import  ClickSession, SessionMkg, Histogram, StoreMap
import sophie.lib.bigbrother.clicksession_db_orm as cl_orm
import logging
import yaml
import array

#Load the logger 
log = logging.getLogger(__name__)

#Session classes for sqlalchemy
#We use this wrapper of sessionmaker so that we reuse the session objects
ClickSessionDbSession = scoped_session(sessionmaker())
PixelDbSession        = scoped_session(sessionmaker())

#Load the configurations for the databases
config = yaml.load(open(SOPHIE_BASE+'/sophie/lib/bigbrother/conifg.yml'))

#Global constants 
#Fist click in the datbase
_liveViewUp = datetime(year=2011,month=03,day=22)

#By ths date the instrumentalization changed from exponential time pinging to two second pinging
_liveViewChange = datetime(year=2011,month=5,day=1)



#Target Database Configuration and access class
csDb = config["db"]["clicksession"]
user = csDb["user"]
password = csDb["password"]
host = csDb["host"]
port = csDb["port"]
db = csDb["db"]
clickSessionsEngine = create_engine("mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, db), pool_recycle=1800, pool_size=10)
ClickSessionDbSession.configure(bind=clickSessionsEngine)

#Pixel database Configuration and session class
pxDb = config["db"]["pixeldb"]
user = pxDb["user"]
password = pxDb["password"]
host = pxDb["host"]
port = pxDb["port"]
db = pxDb["db"]

pixelEngine = create_engine("mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, db), pool_recycle=1800, pool_size=10)
PixelDbSession.configure(bind=pixelEngine)

#Cache of store ids
#Whenever a public id has to be mapped to a store id
#it is searched in the cache. If it is not in the cache, the system requeries the store ids
_storeids = {}

#Selects all click events within a specific time window
def QueryClicksForDateSpan(mintime, maxtime,query_size_limit=1000000):
    session = PixelDbSession()
    clicks = session.query(Click.visitor,\
                           Click.public_id,\
                           Click.id,\
                           Click.time_start,\
                           Click.time_spent,\
                           Click.mkg_id,\
                           Click.mkg_type,\
                           Click.ref,
                           Click.url).\
                           filter(and_(Click.time_start>=mintime,Click.time_start<maxtime)).\
                           order_by(asc(Click.time_start)).limit(query_size_limit)
    return clicks

#Persists the data to the database
def PersistClosedSessions(closedSessions,mkgDataToStore):
    session = ClickSessionDbSession()
    for c in closedSessions:
        session.add(c)
    for m in mkgDataToStore:
        session.add(m)
    session.flush()

"Returns a session object to the database"
def get_clicksession_db_session():
    return ClickSessionDbSession()

"Returns a session object for pixel db"
def get_pixel_db_session():
    return PixelDbSession()

#Create the session tables in the database
def CreateTables():
    base = cl_orm.Base
    #Check if the tables exist and then create.
    #this is the default behavior for creat_all
    base.metadata.create_all(clickSessionsEngine)
    
#Returns the smallest click session ID
def QueryMinSessionId():
    session = ClickSessionDbSession()
    return session.query(func.min(ClickSession.id)).first()
    
#Returns the largest click session ID
def QueryMaxSessionId():
    session = ClickSessionDbSession()
    return session.query(func.max(ClickSession.id)).first()

#Returns a list of ClickSessions where the time span 
def QuerySessionsByTimeLength(min, max):
    session = ClickSessionDbSession()
    return session.query(ClickSession).filter(and_(ClickSession.time_spent<max,ClickSession.time_spent>min))

#Returns the longest session
def QueryMaximumTimeSpent():
    session = ClickSessionDbSession()
    return session.query(func.max(ClickSession.time_spent)).first()

#Returnd the shortest session
def QueryMininmumTimeSpent():
    session = ClickSessionDbSession()
    return session.query(func.min(ClickSession.time_spent)).first()

#Returns all session lenghts
def QueryAllSessionLengths():
    session = ClickSessionDbSession()
    return session.query(ClickSession.time_spent)

#Filter the data by the time start
#def QueryDataByTimeStart(minTs,maxTs):
#    session = ClickSessionDbSession()
#    return session.query(ClickSession.time_spent).filter(and_(CklickSession.time_start<minTs,ClickSession.time_start>=maxTs))
#
def QuerySessions(datetime_min, datetime_max,public_id = None):
    session = ClickSessionDbSession()
    if public_id is None:
        filterand = and_(ClickSession.time_start < datetime_max,ClickSession.time_start>=datetime_min)
    else:
        filterand = and_(ClickSession.time_start < datetime_max,ClickSession.time_start>=datetime_min,ClickSession.public_id==public_id)
    return session.query(ClickSession).filter(filterand)


#Histogram generation and query functionality

#Given a duration, in seconds, and set, returns the latest histogram of the given duration, defaults to one day all stores
def QueryLatestHistogramByDuration(duration=60*60*24,set=0,qualification_certainty_lowerbound=0):
    session = ClickSessionDbSession()
    andexp = and_(Histogram.duration == duration,\
                      Histogram.set == set,\
                      Histogram.qualification_certainty_lb==qualification_certainty_lowerbound)    

    return session.query(Histogram).\
            filter(andexp)\
                    .order_by(desc(Histogram.date)).limit(1).first()
 

#Build a histogram of data in the interval
#this method uses a query to count the sessions that lasted each possible length from [0,maxTimeSpent)
#and which started in the interval [minInt,maxInt)
def QuerySessionCountsInTimeStartWindow(minInt,maxInt,maxTimeSpent,qualification_certainty_lb=0):
    session = ClickSessionDbSession()
    if qualification_certainty_lb == 0:
        andexp = and_(ClickSession.time_start<maxInt,\
                      ClickSession.time_start>minInt,\
                      ClickSession.time_spent<maxTimeSpent)
    else:
        andexp = and_(ClickSession.time_start<maxInt,\
                      ClickSession.time_start>minInt,\
                      ClickSession.time_spent<maxTimeSpent,
                      ClickSession.qualification_certainty>=qualification_certainty_lb)
    return session.query(ClickSession.time_spent, func.count(ClickSession.id))\
            .filter(andexp)\
            .group_by(ClickSession.time_spent)


#Persist day histogram
def PersistHistogram(hist):
    #TODO: Validate histogram
    session = ClickSessionDbSession()
    session.add(hist)
    session.flush()

#Returns the histogram arrays for the date range
def QueryHistogramsForDateRange(date_min,date_max,set=0,duration=60*60*24,qualification_certainty_lowerbound=0):
    session = ClickSessionDbSession()
    andexp = and_(Histogram.date<date_max,Histogram.date>=date_min,Histogram.set==set,Histogram.qualification_certainty_lb==qualification_certainty_lowerbound)
    return session.query(Histogram.histogram).filter(andexp)


def QueryStoresWithSessions():
    session = ClickSessionDbSession()
    return session.query(func.distinct(ClickSession.public_id))
#Returns the ids of all mapped stores

def QueryListOfMappedStoreIds():
    session = ClickSessionDbSession()
    return session.query(StoreMap.store_id)

def QuerySessionsWithQualification(qual):
    session = ClickSessionDbSession()
    return session.query(ClickSession).filter(ClickSession.qualification_certainty==qual)

def ForceFlush():
    session = ClickSessionDbSession()
    session.flush()
