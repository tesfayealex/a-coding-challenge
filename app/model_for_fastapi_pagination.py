
from pydantic import BaseModel
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, JSON

Base = declarative_base()

class Planning(Base):

    __tablename__ = "planning"

    id = Column(Integer,primary_key=True)
    originalId = Column(String)
    talentId = Column(String)
    talentName = Column(String)
    talentGrade = Column(String)
    bookingGrade = Column(String)
    operatingUnit = Column(String)
    officeCity = Column(String)
    officePostalCode = Column(String)
    jobManagerName = Column(String)
    jobManagerId = Column(String)
    totalHours = Column(Float)
    clientName = Column(String)
    clientId = Column(String)
    industry = Column(String)
    requiredSkills = Column(JSON)
    optionalSkills = Column(JSON)
    isUnassigned = Column(Boolean)

class PlanningOut(BaseModel):
    id : int
    originalId : str
    talentId : str
    talentName : str
    talentGrade : str
    bookingGrade : str
    operatingUnit : str
    officeCity : str
    officePostalCode : str
    jobManagerName : str
    jobManagerId : str
    totalHours : float
    clientName : str
    clientId : str
    industry : str
    requiredSkills : object
    optionalSkills : object
    isUnassigned : bool

    class Config:
        orm_mode = True