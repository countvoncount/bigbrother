#Orm file for the sessions db 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean

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
    closed      = Column(Boolean)

    def __init__(self,id=None,
                 firstclick=None,
                 public_id=None,
                 mkg_id=None,
                 mkg_type=None,
                 visitor=None,
                 time_start=None,
                 time_spent=None,
                 click_count=None,
                 closed=None):
        Base.__init__(self)
        self.id = id
        self.firstclick = firstclick
        self.public_id = public_id
        self.mkg_id = mkg_id
        self.mkg_type =mkg_type
        self.visitor = visitor
        self.time_start = time_start
        self.time_spent = time_spent
        self.click_count = click_count

class SessionMkg(Base):
    __tablename__='SessionMkg'
    id           =Column(Integer,nullable=False,primary_key=True)
    session_id   =Column(Integer, ForeignKey('ClickSession.id'),nullable=False)
    mkg_id       =Column(Unicode(length=255),nullable=False)
    mkg_type     =Column(Unicode(length=16))
    first        =Column(Boolean)


