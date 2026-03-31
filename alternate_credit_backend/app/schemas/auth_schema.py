from pydantic import BaseModel, EmailStr, Field


class CustomerSignupRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=150)
    email: EmailStr
    password: str = Field(..., min_length=6)
    phone: str | None = None


class CustomerSigninRequest(BaseModel):
    email: EmailStr
    password: str




class AuthResponse(BaseModel):
    message: str
    public_user_id: str
    name: str
    email: EmailStr
    role: str