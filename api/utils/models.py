from utils import Base

from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import relationship

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)

class CompanyMetadata(Base):
    __tablename__ = "company_metadata"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    year = Column(Integer)