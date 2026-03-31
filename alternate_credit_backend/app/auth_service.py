import uuid
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models.user_models import User
from app.schemas.auth_schema import CustomerSignupRequest, CustomerSigninRequest
from app.utils.security import hash_password, verify_password


def signup_customer(payload: CustomerSignupRequest, db: Session):
    # check if email already exists
    existing_user = db.query(User).filter(User.email == payload.email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # create user
    new_user = User(
        public_user_id=str(uuid.uuid4()),
        name=payload.name,
        email=payload.email,
        phone=payload.phone,
        password_hash=hash_password(payload.password),
        role="customer"
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {
        "message": "Signup successful",
        "public_user_id": new_user.public_user_id,
        "name": new_user.name,
        "email": new_user.email,
        "role": new_user.role
    }
def signin_manager(payload: CustomerSigninRequest, db: Session):
    user = db.query(User).filter(User.email == payload.email).first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if user.role != "manager":
        raise HTTPException(status_code=403, detail="Not a manager account")

    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return {
        "message": "Manager signin successful",
        "public_user_id": user.public_user_id,
        "name": user.name,
        "email": user.email,
        "role": user.role
    }


def signin_customer(payload: CustomerSigninRequest, db: Session):
    user = db.query(User).filter(User.email == payload.email).first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if user.role != "customer":
        raise HTTPException(status_code=403, detail="Not a customer account")

    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return {
        "message": "Signin successful",
        "public_user_id": user.public_user_id,
        "name": user.name,
        "email": user.email,
        "role": user.role
    }