from pydantic import BaseModel, Field
from typing import Dict, Optional


class UserBase(BaseModel):
    user_id: int = Field(..., alias="id")
    first_name: str = Field(..., alias="firstName")
    last_name: str = Field(..., alias="lastName")
    gender: str
    age: int


class UserAddress(BaseModel):
    street: Optional[str] = Field(None, alias="address")
    city: Optional[str] = None
    postal_code: Optional[str] = Field(None, alias="postalCode")


class UserSchema(UserBase):
    address: Dict = Field(...)

    class Config:
        populate_by_name = True

    def flatten_address(self) -> dict:
        address_obj = UserAddress(**self.address)
        return {
            "user_id": self.user_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "gender": self.gender,
            "age": self.age,
            "street": address_obj.street,
            "city": address_obj.city,
            "postal_code": address_obj.postal_code
        }
