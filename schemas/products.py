from pydantic import BaseModel, Field


class ProductSchema(BaseModel):
    product_id: int = Field(..., alias="id")
    name: str = Field(..., alias="title")
    category: str
    brand: str
    price: float

    class Config:
        populate_by_name = True

    def get_cleaned_data(self) -> dict:
        if self.price > 50:  # Filter out products with price less than 50
            return {
                "product_id": self.product_id,
                "name": self.name,
                "category": self.category,
                "brand": self.brand,
                "price": self.price
            }
        return None