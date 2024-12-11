import pytest
from schemas.users import UserSchema
from schemas.products import ProductSchema
from schemas.carts import CartSchema


def test_user_flatten_address():
    user_data = {
        "id": 1,
        "firstName": "Ian",
        "lastName": "Daniel",
        "gender": "Male",
        "age": 25,
        "address": {
            "address": "liwal",
            "city": "Nairobi",
            "postalCode": "00100",
        }
    }

    user = UserSchema(**user_data)
    flattened = user.flatten_address()

    assert flattened["user_id"] == 1
    assert flattened["first_name"] == "Ian"
    assert flattened["street"] == "liwal"
    assert flattened["city"] == "Nairobi"
    assert flattened["postal_code"] == "00100"


def test_removal_of_cheap_products():
    product_1 = {
        "id": 1,
        "title": "Product 1",
        "category": "Category 1",
        "brand": "Brand 1",
        "price": 100
    }

    product_2 = {
        "id": 2,
        "title": "Product 2",
        "category": "Category 2",
        "brand": "Brand 2",
        "price": 20
    }

    product1 = ProductSchema(**product_1)
    product2 = ProductSchema(**product_2)

    assert product1.get_cleaned_data() is None
    assert product2.get_cleaned_data() is not None


def test_cart_calculations():
    cart_data = {
        "id": 1,
        "userId": 1,
        "products": [
            {
                "id": 1,
                "quantity": 2,
                "price": 50
            },
            {
                "id": 2,
                "quantity": 3,
                "price": 20
            }
        ]
    }

    cart = CartSchema(**cart_data)
    flattened_products = cart.flatten_products()

    assert len(flattened_products) == 2
    assert flattened_products[0]["total_cart_value"] == 100