USER_SUMMARY_QUERY = """
SELECT
  u.user_id,
  u.first_name,
  u.age,
  u.city,
  SUM(c.quantity) as total_items,
  ROUND(SUM(c.total_cart_value), 2) as total_spent
FROM
  `dummyjson.users` u
INNER JOIN
  `dummyjson.carts` c
ON
  u.user_id = c.user_id
GROUP BY
  u.user_id,
  u.first_name,
  u.age,
  u.city
ORDER BY
  total_spent DESC
"""

CATEGORY_SUMMARY_QUERY = """
SELECT
  p.category,
  ROUND(SUM(c.total_cart_value), 2) AS `total_sales`,
  SUM(c.quantity) AS `total_items_sold`
FROM
  `dummyjson.carts` c
INNER JOIN
  `dummyjson.products` p
ON
  p.product_id=c.product_id
GROUP BY category
ORDER BY total_sales DESC
"""

CART_DETAILS_QUERY = """
SELECT cart_id, user_id, product_id, quantity, price, total_cart_value FROM `dummyjson.carts`
"""
