from sqlalchemy.orm import Session
from utils import models, schemas, common


def get_user(db: Session, user_id: int):
    return db.query(models.User).filter(models.User.id == user_id).first()


def get_user_by_username(db: Session, username: str):
    return db.query(models.User).filter(models.User.username == username).first()


def validate_user(db: Session, user: schemas.UserCreate):
    hashed_password = common.get_hashed_password(user.password).decode('utf-8')
    return db.query(models.User).filter(models.User.username == user.username and models.User.hashed_password == hashed_password).first()


def get_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.User).offset(skip).limit(limit).all()


def validate_access_token(db: Session, access_token: str):
    decoded_data = common.decode_token(access_token)
    common.compare_time(decoded_data["exp"])
    username = decoded_data["username"]
    hashed_password = decoded_data["password"]
    return db.query(models.User).filter(
        models.User.username == username and
        models.User.hashed_password == hashed_password).first()

def validate_openai_api_key(api_key):
    return True

def create_user(db: Session, user: schemas.UserCreate):
    hashed_password = common.get_hashed_password(user.password).decode('utf-8')
    db_user = models.User(username=user.username,
                          hashed_password=hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def metadata_fetch_company_years(db: Session, skip: int = 0, limit: int = 100):
    db_metadata = db.query(models.CompanyMetadata).offset(skip).limit(limit).all()

    company_names_years = [[row.name, row.year] for row in db_metadata]
    return company_names_years

def store_company_metadata(db: Session, metadata: schemas.CompanyMetadata):
    for name in metadata.company_names_years:
        for year in metadata.company_names_years[name]:
            d = models.CompanyMetadata(name=name,
                                       year=year)
            db.add(d)
    db.commit()
