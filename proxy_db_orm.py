"""
Author Santiago Akle
Partial orm for the proxy db
This is intended to be used to extract subsets of the 
store data across all stores.
"""

#Orm file for the sessions db 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean,Date,PickleType,UniqueConstraint,UnicodeText

#SQL alchemy orm for this table
Base = declarative_base()

class ClickSession(Base):
    __tablename__ = 'ClickSession'
    id          = Column(Integer, nullable=False, primary_key=True)
    firstclick  = Column(Integer, nullable=False)
    public_id   = Column(Unicode(length=16))
    time_start  = Column(DateTime)
    time_spent  = Column(Integer, default=0)
    visitor     = Column(Unicode(length=16))
    has_mkg     = Column(Boolean)
    click_count  = Column(Integer,default=1)
    order_id    =Column(Integer,nullable=True)
    ref = Column(UnicodeText)
 
