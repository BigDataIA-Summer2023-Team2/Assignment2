from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy.orm import relationship
from utils import models, engine, SessionLocal, schemas, crud, common, pipeline

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Transcript Insight")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/api/v1/health")
async def check() -> dict:
    return {"message": "OK"}


@app.post("/api/v1/user/register", response_model=schemas.User)
async def register_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    if not (user.username and
            user.password and
            user.cnf_password):
        raise HTTPException(
            status_code=404, detail=r"Username and password cannot be empty")
    if user.password != user.cnf_password:
        raise HTTPException(
            status_code=404, detail=r"Provided passwords do not match")
    db_user = crud.get_user_by_username(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    return crud.create_user(db=db, user=user)


@app.post("/api/v1/user/generate_key")
async def generate_api_key(user: schemas.UserCreate, db: Session = Depends(get_db)):
    if not (user.username and
            user.password and
            user.cnf_password):
        raise HTTPException(
            status_code=404, detail=r"Username and password cannot be empty")
    db_user = crud.validate_user(db, user=user)
    if not db_user:
        raise HTTPException(
            status_code=404, detail="Forbidden, username and passowrd mismatch")
    data_to_encode = {
        "username": user.username,
        "password": common.get_hashed_password(user.password).decode('utf-8')
    }
    access_token = common.create_access_token(data_to_encode)
    return {"API_ACCESS_TOKEN": access_token}

@app.post("/api/v1/company_metadata/store")
async def store_company_metadata(metadata: schemas.CompanyMetadata, db: Session = Depends(get_db)):
    crud.store_company_metadata(db, metadata=metadata)
    return {"message": "Metadata stored successfully"}

@app.get("/api/v1/company_metadata/fetch")
async def store_company_metadata(db: Session = Depends(get_db)):
    fetched = crud.metadata_fetch_company_years(db)
    return {"company_names_years": fetched}

@app.post("/api/v1/transcripts/embedd")
async def run_dag(userInput: schemas.EmbeddTranscript, db: Session = Depends(get_db)):
    crud.validate_access_token(db=db, access_token=userInput.api_key)
    crud.validate_openai_api_key(api_key=userInput.openai_api_key)
    response = pipeline.trigger_fetch_transcript(userInput=userInput)
    return response

@app.get("/api/v1/transcripts/fetch")
async def fetch_transcript(userInput: schemas.FetchTranscript, db: Session = Depends(get_db)):
    crud.validate_access_token(db=db, access_token=userInput.api_key)
    response = pipeline.fetch_transcript(userInput=userInput)
    return response

@app.post("/api/v1/dag/fetch_metadata")
async def run_dag_fetch_metadata():
    response = pipeline.trigger_fetch_metadata_dag()
    return response

@app.get("/api/v1/transcripts/query")
async def query(userInput: schemas.QueryTranscript, db: Session = Depends(get_db)):
    crud.validate_openai_api_key(api_key=userInput.openai_api_key)
    result = pipeline.get_vss_results(userInput.query, userInput.embedding, userInput.openai_api_key)
    return {"summary": result}
    
@app.get("/api/v1/transcripts/query_year")
async def query(userInput: schemas.QueryTranscriptYear, db: Session = Depends(get_db)):
    crud.validate_openai_api_key(api_key=userInput.openai_api_key)
    result = pipeline.get_vss_hybrid_year_results(userInput.query, userInput.start_year, userInput.end_year, userInput.embedding, userInput.openai_api_key)
    return {"summary": result}

@app.get("/api/v1/transcripts/query_company")
async def query(userInput: schemas.QueryTranscriptCompany, db: Session = Depends(get_db)):
    crud.validate_openai_api_key(api_key=userInput.openai_api_key)
    result = pipeline.get_vss_hybrid_company_results(userInput.query, userInput.company, userInput.embedding, userInput.openai_api_key)
    return {"summary": result}