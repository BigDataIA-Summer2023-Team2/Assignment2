import time
from jose import jwt
from datetime import datetime, timedelta
import os
import bcrypt

SECRET_KEY = os.getenv("SECRET_KEY", "")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30))


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def get_hashed_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


def decode_token(access_token):
    decoded_token = jwt.decode(
        access_token, SECRET_KEY, algorithms=[ALGORITHM])
    return decoded_token


def compare_time(token_time: int):
    if int(time.time()) < token_time:
        return True
    else:
        return False
