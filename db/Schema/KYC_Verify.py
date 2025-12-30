from pydantic import BaseModel, Field, validator, EmailStr
from typing import List, Optional, Literal
from datetime import datetime, date

from random import randint


class KYCOTPRequest(BaseModel):
    mobile: str
    email: EmailStr

class KYCOTPVerifyRequest(BaseModel):
    mobile: str
    email: EmailStr
    otp: str
    