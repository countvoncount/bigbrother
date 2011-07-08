#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This file contains the database access layer for the
proxy db
"""
import os,sys
SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
sys.path.append(SOPHIE_BASE)

from sqlalchemy import create_engine,func,and_,asc,MetaData
from sqlalchemy.orm import sessionmaker,mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean,Table
from sqlalchemy.sql.expression import desc 
import paste,pickle
from datetime import timedelta,datetime,date

import sophie.lib.bigbrother.clicksession_db_orm as cl_orm
import logging
import yaml
import array

#Load the logger 
log = logging.getLogger(__name__)

#Session classes for sqlalchemy
ProxyDbSession = sessionmaker()

#Load the configurations for the databases
config = yaml.load(open(SOPHIE_BASE+'/sophie/lib/bigbrother/conifg.yml'))


#Some general variables
_firstStoreDate = datetime(year=2010,month=04,day=27)

#Target Database Configuration and access class
pxDb = config["db"]["proxydb"]
user = pxDb["user"]
password = pxDb["password"]
host = pxDb["host"]
port = pxDb["port"]
db = pxDb["db"]
proxySessionsEngine = create_engine("mysql://{0}:{1}@{2}:{3}/{4}?charset=utf8".format(user, password, host, port, db), pool_recycle=1800, pool_size=10)
metadata = MetaData(proxySessionsEngine)


#Load the tables
storeAccounts = Table('StoreAccounts',metadata, autoload=True)

#Make an empty class deffinition for sqlalchemy to alter and define the structure
class StoreAccounts(object):
    pass
#Execute the mapping
storeMapper = mapper(StoreAccounts,storeAccounts)

#Query all the store ids and public ids and creation dates for stores created after the parameter
def QueryAllStoreIdPIDPAirsCreatedAfterDate(minDateTime):
    session=ProxyDbSession()
    return session.query(StoreAccounts.id,StoreAccounts.publicId,StoreAccounts.created).\
            filter(StoreAccounts.created>minDateTime)



