from pydantic import BaseModel, Field


class CartProductSchema(BaseModel):
    product_id: int = Field(..., alias="id")

    quantity: int
    price: float

    def get_total_cart_value(self) -> float:
        total = self.quantity * self.price
        return round(total, 2)


class CartSchema(BaseModel):
    cart_id: int = Field(..., alias="id")
    user_id: int = Field(..., alias="userId")
    products: list[CartProductSchema]

    def flatten_products(self) -> list[dict]:
        flattened_products = []
        for product in self.products:
            flattened_products.append({
                "cart_id": self.cart_id,
                "user_id": self.user_id,
                "product_id": product.product_id,
                "quantity": product.quantity,
                "price": product.price,
                "total_cart_value": product.get_total_cart_value()
            })
        return flattened_products
