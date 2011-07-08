#!/usr/bin/env python

"""
This module contains methods to processes the click database entries.
It creates the clicksession database entries to do this 
it processes the clicks sequentially from the 
start of the live view time to the present time.
When it finishes it leaves a state picke of the sessions
that remained open.
It the pickle exists then it begins from the latest
processed click and continues to buid the sessions.

The results of the process are stored in the
click session database.
"""

import os,sys
SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
sys.path.append(SOPHIE_BASE)


from sqlalchemy import create_engine,func,and_,asc
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sophie.lib.connectors.vurve.db.merchant_db_orm import StoreAccount
from sophie.lib.connectors.vurve.db.pixel_db_orm import Click
from sophie.lib.bigbrother.clicksession_db_orm import  ClickSession, SessionMkg
import sophie.lib.bigbrother.clicksession_db as clickdb
from datetime import timedelta,datetime,date
import paste,pickle
import logging
import yaml
import re

#Load the logger 
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)


#Some global variables
session_timeout = timedelta(hours=2)

#Load the configurations for the databases
config = yaml.load(open('conifg.yml'))
pklConfig = config["clickchurn"]

#Load the path of the open session pickle
_openSessionPth = pklConfig["opensessionpath"]

#List of compiled regular expressions that indicate
#That the url is an order confirmation page
_OrderConfirmationMatchers = []
_OrderConfirmationMatchers.append(re.compile("Invoice\?smode=confirm"))
_OrderConfirmationMatchers.append(re.compile("ysco\.confirm"))

#If these keywords show up they should not be stored in the database
_skip_keywords = ['nil','{keyword:nil}'];

#Skip the clicks if the user is in the user skip list
_skip_visitor = ['']
#skip the click if the store is in the store skip list
_skip_store = ['']

#This method processes a non empty list of clicks, and a list of open sessions
#if a session times out it closes it, if a session receives another click it extends it
#if a new session is found it creates it. 

#If a click contains mkg_id it calls the filter mkg method to 
#decide if the mkg_id has to be associated to the session
def process_click_query(click_query,openSessions,sessionTimeout):
    
    #List of sessions closed during the processing of this click group
    closedSessions = list();
    mkgDataToStore   = list();
     
    lastTime = None

    for c in click_query:
        #Check if this click goes to an order confirmation page, if so we will store it for the session
        order_confirmation = FilterOrderConfirmation(c.url)
        #If it has an order confirmation url then the certainty that it is qualified traffic is 1
        if order_confirmation is not None:
            qualification_certainty = 1
        else:
            qualification_certainty = 0

        pSes = None
        #if c.time_spent > 3600:
        #    print "Loong click {0},{1},{2} ".format(c.time_spent,c.visitor,c.public_id)
        #Get the visitor dictionary by the key index
        vidct = openSessions.get(c.public_id)
        
        #First open session for this store
        if vidct == None:
            vidct = {}
            pSes = ClickSession(firstclick=c.id,\
                                                public_id=c.public_id,\
                                                visitor=c.visitor,\
                                                time_start=c.time_start,\
                                                time_spent=c.time_spent,\
                                                click_count=1,\
                                                ref=c.ref,\
                                                order_confirmation=order_confirmation,\
                                                qualification_certainty=qualification_certainty)
            vidct[c.visitor] = pSes
            openSessions[c.public_id]=vidct
            
        else: 
            #This store has an open session
            #Get the visitor session
            pSes = vidct.get(c.visitor);
            #This visitor does not have a session in this store
            if pSes == None:
                 pSes = ClickSession(firstclick=c.id,\
                                                public_id=c.public_id,\
                                                visitor=c.visitor,\
                                                time_start=c.time_start,\
                                                time_spent=c.time_spent,\
                                                click_count=1,\
                                                ref=c.ref,\
                                                order_confirmation=order_confirmation,\
                                                qualification_certainty=qualification_certainty)
                 vidct[c.visitor] = pSes
            else:
                #If the last time the session was active is less than the actual start_time minus the 
                #session time out parameter, this click starts a new session and the existing one has to be closed
                if pSes.time_start+timedelta(seconds=pSes.time_spent) < c.time_start - sessionTimeout:
                   closedSessions.append(pSes)
                   pSes = ClickSession(firstclick=c.id,\
                                                public_id=c.public_id,\
                                                visitor=c.visitor,\
                                                time_start=c.time_start,\
                                                time_spent=c.time_spent,\
                                                click_count=1,\
                                                ref=c.ref,\
                                                order_confirmation=order_confirmation,\
                                                qualification_certainty=qualification_certainty)
                    
                   vidct[c.visitor] = pSes
                else:
                    #If this click has an order confirmation, save it 
                    if order_confirmation is not None:
                        pSes.order_confirmation = order_confirmation
                        pSes.qualification_certainty = qualification_certainty

                    #This click is another click in the session then extend the sessions time_spent
                    pSes.time_spent = (c.time_start + timedelta(seconds=c.time_spent) - pSes.time_start).seconds
                    #Increment the click count
                    pSes.click_count+=1
                    #print "Extending session start time {0}, Time Spent {1}\n".format(pSes.time_start,pSes.time_spent)
                    #if pSes.time_spent > 20000:
                    #    print "Large extension, last click {0},{1},{2}".format(c.time_spent,c.visitor,c.public_id)
       
      
       #Check if there is a new mkg_id and add it to the other table
        if c.mkg_id != None:
            smk = SessionMkg()
            smk.session_id = c.id
            smk.mkg_id = c.mkg_id
            smk.mkg_type = c.mkg_type
            if not pSes.has_mkg:
                smk.first = True
                pSes.has_mkg = True
            if FilterMkgType(smk,skip_kw):
                mkgDataToStore.append(smk) 
       
        #This variable is assigned to the last time start when the loop exits
        lastTime = c.time_start
    #List of store dictionaries to remove
    removeDicts = []
    #After all clicks have been processed iterate over all open sessions and close the ones that have timed out
    for kvd,vd in openSessions.iteritems():
        #list of sessions to remove
        removeKeys = [];
        for k,s in vd.iteritems():
            if s.time_start+timedelta(seconds=s.time_spent) < lastTime - sessionTimeout:
                #The session has timed out, close it
                closedSessions.append(s)
                #Remove it from the open session
                removeKeys.append(k)
        #Now remove the keys 
        for k in removeKeys:
           del vd[k]
        #If we emptied the inner dictionary remove it
        if len(vd) == 0:
            removeDicts.append(kvd)

    #Remove the store dictionaries
    for k in removeDicts:
         del openSessions[k]
        
    return closedSessions,mkgDataToStore,lastTime
    
#If the url is an order confirmation page we return it, else return null
def FilterOrderConfirmation(url):
    for regex in _OrderConfirmationMatchers:
        if regex.search(url) is not None:
            return url
    return None


#This method defines rules to filter some click mkg ids
def FilterMkgType(smk):
    if smk.mkg_type == 'kw' and smk.mkg_id in _skip_keywords:
        return False
    else:
        return True

"""Loads the pickle of open session defined in the config file,
processes the clicks that arrived since the last click processed.
To produce the ClickSession data, which is persited in the database"""
def ChurnClicksToToday(): 
    log.info("Churning clicks to produce session data until {0}".format(datetime.utcnow().date()))
    #Create the tables if they dont exist
    clickdb.CreateTables()
 
    if os.path.exists(_openSessionPth):
        log.debug("Pickle with open sessions exists at {0}".format(_openSessionPth))
        (openSessions,datetime_start) = pickle.load(open(_openSessionPth,'r'))
    else:
        log.debug("No pickle with sessions found at {0}. Will churn all clicks since {1}".\
                   format(_openSessionPth,clickdb._liveViewUp))
        #If the pickle does not exist then we need to start from the begining
        openSessions = {}
        datetime_start = clickdb._liveViewUp 
   
    #Get today's first second datetime 
    utcTodayDate = datetime.utcnow().date()
    datetime_end = datetime(utcTodayDate.year,utcTodayDate.month,utcTodayDate.day)
   
    timedelta_increment = timedelta(days=1)

    #Main iteration loop
    query_set = get_click_set(datetime_start,datetime_end,timedelta_increment)
    log.debug("Churning Clicks From {0} to {1} ".format(datetime_start,datetime_end))
    itercount = 1
    for click_query in query_set:
        log.debug("processing for period {0}, {1} clicks".format(itercount,click_query.count()))
        if click_query.count() != 0:
            closedSessions,mkgDataToStore,latest_click = process_click_query(click_query,openSessions,session_timeout)
        itercount +=1
            
    #Save the open sessions 
    pickle.dump((openSessions,latest_click),open(_openSessionPth,'wb'))
    
"""Returns a generator that will iteratively return queries with the 
click sessions in date intervals of length datetime delta"""
def get_click_session_set(datetime_start,datetime_end,datetime_delta):
    act_date = datetime_start
    session = clickdb.get_session()
    
    while act_date < datetime_end:
        end_date = min(datetime_end,act_date+datetime_delta);
        andstatement = and_(ClickSession.time_start>=act_date,ClickSession.time_start<datetime_end)
        yield session.query(ClickSession).filter(andstatement)
        act_date = act_date+datetime_delta;

"""Returns the generator that iterates over query sets which contain all
clicks in intervals of length datetime_delta srtarting at datetime_start"""
def get_click_set(datetime_start,datetime_end,datetime_delta):
    act_date = datetime_start
    while act_date < datetime_end:
        end_date = min(datetime_end,act_date+datetime_delta);
        session = clickdb.get_pixel_db_session()
        andstatement = and_(Click.time_start>=act_date,Click.time_start<end_date)
        yield session.query(Click).filter(andstatement)
        act_date = act_date+datetime_delta;


"""This method extracts the set of sessions which have been flaged as 
qualified traffic and looks for all clicksessions which have visitor,store pairs
which also correspond to a qualified session"""

def ExtendQualification():
   #Aquire the click sessions and add them to a hashtable"
   qry = clickdb.QuerySessionsWithQualification(1)
   s = set();
   for cs in qry:
       s.add((cs.visitor,cs.public_id))
   
   #Iterate over all sessions and generate the list of those that need updating"
   timedelta_length = timedelta(days=1)
   datetime_start = clickdb._liveViewChange;
   datetime_end   = datetime.utcnow();
   
   sessionDays = get_click_session_set(datetime_start,datetime_end,timedelta_length)
   iter = 1
   updated_counter = 0;
   tic = datetime.now()
   for q in sessionDays:
       q_count = q.count()
       log.debug("Queried {0} sessions for {1}".format(q_count,iter))
       for cs in q:
            if (cs.visitor,cs.public_id) in s:
                if cs.qualification_certainty != 1:
                    cs.qualification_certainty = 0.5
                    updated_counter +=1;

       clickdb.ForceFlush()
       del q
       toc = datetime.now()-tic
       tic = datetime.now()
       seconds_per_clicksession = float(toc.seconds)/q_count
       log.debug("Processed {0} updated so far {1}, seconds per click session {2} ".format(iter,updated_counter,seconds_per_clicksession))
       iter+=1
   
                

if __name__=="__main__":
    ChurnClicksToToday()
    ExtendQualification()
