#!/usr/bin/env python
## -*- coding: utf-8 -*-

import os,sys
SOPHIE_BASE = os.environ.get('VURVE_SOPHIE_BASE','/Users/tiago/vurve/code/sophie');
sys.path.append(SOPHIE_BASE)

import sophie.lib.bigbrother.clicksession_db as db
import sophie.lib.bigbrother.histograms as htool
import sophie.lib.bigbrother.clicksession_db as clickdb
from sophie.lib.bigbrother.clicksession_db_orm import ClickSession
from sqlalchemy import and_
import matplotlib
#supress window
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import pickle
import csv
import datetime
import array
import operator

_histogramBinCount = htool._maxUsefulLength

def MakePlotAndSave(list_histogram,path):
    
    print "Histogram bin count {0} list size {1}".format(_histogramBinCount,len(list_histogram))
    plt.figure()
    plt.bar(range(_histogramBinCount),list_histogram)
    plt.savefig(path)

def MakeShorterHist(list_histogram,path):
    hist = [0,]*200;

    for i in range(200):
        hist[i]=sum(list_histogram[i*10:(i*10+9)])
    
    plt.figure()
    plt.bar(map(operator.sub,map(operator.mul,range(200),[10,]*200),[5,]*200),hist)
    plt.savefig(path)


def query_and_plot_histogram_for_all(path):
    session = clickdb.get_clicksession_db_session()

    qry = session.query(ClickSession.time_spent).filter(and_(ClickSession.qualification_certainty>=0,ClickSession.time_spent<=2000))
    print "{0} data points queried ".format(qry.count())
    dat = [x[0] for x in qry]
    plt.hist(dat,bins=200)
   
    qry = session.query(ClickSession.time_spent).filter(and_(ClickSession.qualification_certainty>=0.5,ClickSession.time_spent<=2000))
    print "{0} data points queried ".format(qry.count())
    dat = [x[0]*100 for x in qry]
    plt.hist(dat,bins=200)
    

    qry = session.query(ClickSession.time_spent).filter(and_(ClickSession.qualification_certainty>=1,ClickSession.time_spent<=2000))
    print "{0} data points queried ".format(qry.count())
    dat = [x[0]*1000 for x in qry]
    plt.hist(dat,bins=200)
    plt.savefig(path)
  
def query_and_plot(and_statement_query_pairs):
    session = clickdb.get_clicksession_db_session()
    
    for tuple in and_statement_query_pairs:
        qry = session.query(ClickSession.time_spent).filter(tuple[0])
        dat = [x[0] for x in qry]
        print "Data size {0} for histogram {1} ".format(len(dat),tuple[1])
        plt.figure()
        plt.hist(dat,bins=200)
        plt.savefig(tuple[1])



if __name__ == '__main__':
    and_s = [and_(ClickSession.click_count==i+1,ClickSession.time_spent<2000) for i in range (5)]
    and_s.extend([and_(ClickSession.click_count==i+1,ClickSession.time_spent<2000,ClickSession.qualification_certainty >= 0.5) for i in range (5)])

            
    paths = ["./Images/Hist{0}.eps".format(i+1) for i in range(5)]
    paths.extend(["./Images/Hist{0}Qual.eps".format(i+1) for i in range(5)])

    query_and_plot(zip(and_s,paths))

