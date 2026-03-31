from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas.auth_schema import CustomerSignupRequest, CustomerSigninRequest, AuthResponse
from app.auth_service import signup_customer, signin_customer, signin_manager  # ✅ correct import

router = APIRouter()

@router.post("/manager/signin", response_model=AuthResponse)
def manager_signin(payload: CustomerSigninRequest, db: Session = Depends(get_db)):
    return signin_manager(payload, db) 

@router.post("/customer/signup", response_model=AuthResponse)
def customer_signup(payload: CustomerSignupRequest, db: Session = Depends(get_db)):
    return signup_customer(payload, db)   # ✅ FIXED


@router.post("/customer/signin", response_model=AuthResponse)
def customer_signin(payload: CustomerSigninRequest, db: Session = Depends(get_db)):
    return signin_customer(payload, db)   # ✅ FIXED