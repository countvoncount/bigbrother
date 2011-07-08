#Orm file for the sessions db 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,Unicode,DateTime,ForeignKey,Boolean,Date,PickleType,UniqueConstraint,UnicodeText,Float

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
    order_confirmation =Column(UnicodeText)
    qualification_certainty = Column(Float)
 
    def __init__(self,id=None,\
                 firstclick=None,\
                 public_id=None,\
                 mkg_id=None,\
                 mkg_type=None,\
                 visitor=None,\
                 time_start=None,\
                 time_spent=None,\
                 click_count=None,\
                 order_id=None,\
                 ref=None,\
                 order_confirmation=None,\
                 qualification_certainty=None):
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
        self.order_id   = order_id
        self.ref = ref
        self.order_confirmation = order_confirmation
        self.qualification_certainty=qualification_certainty

class StoreMap(Base):
    __tablename__='StoreMap'
    public_id =Column(Unicode(length=8),nullable=False)
    store_id  =Column(Integer,nullable=False,primary_key=True)

class SessionMkg(Base):
    __tablename__='SessionMkg'
    id           =Column(Integer,nullable=False,primary_key=True)
    session_id   =Column(Integer, ForeignKey('ClickSession.id'),nullable=False)
    mkg_id       =Column(Unicode(length=255),nullable=False)
    mkg_type     =Column(Unicode(length=16))
    first        =Column(Boolean)

class Histogram(Base):
    __tablename__='Histogram'
    id           =Column(Integer,primary_key=True)
    date         =Column(DateTime,nullable=False)
    duration     =Column(Integer,nullable=False) #Length of the interval in seconds over which the session starts are selected
    set          =Column(Integer) #This field is 0 for the histogram over all stores
    qualification_certainty_lb  =Column(Float) #This indicates that the sessions taken into account had at least this qualification_certainty
    histogram    =Column(PickleType) #Histograms are pickled arrays
    __table_args__ = (UniqueConstraint("date", "set", "qualification_certainty_lb"), {})

