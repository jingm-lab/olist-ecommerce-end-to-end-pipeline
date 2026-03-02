import psycopg2
import random
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from collections import defaultdict

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    dbname=os.getenv("POSTGRES_DB"),
)

cursor = conn.cursor()

cursor.execute(
    """
        SELECT DISTINCT customer_id FROM customers
"""
)
customer_ids = [id[0] for id in cursor.fetchall()]

# print(customer_ids[-5:])

cursor.execute(
    """
        SELECT DISTINCT seller_id FROM order_items
"""
)
seller_ids = [id[0] for id in cursor.fetchall()]
# print(seller_ids[:5])

cursor.execute(
    """
        SELECT 
            product_id,
            MIN(price) AS min_price, 
            MAX(price) AS max_price,
            MIN(freight_value) AS min_freight,
            MAX(freight_value) AS max_freight
        FROM order_items
        GROUP BY product_id
"""
)
product_data = cursor.fetchall()
# print(product_data[:2])

product_dict = {}

for product_tuple in product_data:
    product_dict[product_tuple[0]] = {
        "min_price": float(product_tuple[1]),
        "max_price": float(product_tuple[2]),
        "min_freight": float(product_tuple[3]),
        "max_freight": float(product_tuple[4]),
    }

# print(next(iter(product_dict.items())))

cursor.execute(
    """
    SELECT DISTINCT seller_id, product_id
    FROM order_items
"""
)
seller_prod_data = cursor.fetchall()

seller_prod_dict = defaultdict(list)

for seller_prod_tuple in seller_prod_data:
    seller_prod_dict[seller_prod_tuple[0]].append(seller_prod_tuple[1])
# print(next(iter(seller_prod_dict.items())))

order_statuses = ["processing", "invoiced"]
start = datetime(2025, 1, 1)
end = datetime(2025, 12, 1, 23, 59, 59)

# Total seconds between 2025/01/01 and 2025/12/31
diff = int((end - start).total_seconds())

transaction_num = 10  # Generate 10 transaction entries
success_counter = 0

try:
    for i in range(transaction_num):
        skip_order = False
        # Simulate data for orders table
        order_id = uuid.uuid4().hex

        customer_id = random.choice(customer_ids)
        num_products = random.randint(1, 2)
        selected_product_ids = []
        selected_seller_ids = []
        for _ in range(num_products):
            seller_id = random.choice(seller_ids)
            product_id = random.choice(seller_prod_dict[seller_id])

            selected_seller_ids.append(seller_id)
            selected_product_ids.append(product_id)

        order_status = random.choice(order_statuses)
        random_seconds_for_purchase = random.randint(0, diff)
        order_purchase_timestamp = start + timedelta(
            seconds=random_seconds_for_purchase
        )

        random_seconds_for_approval = random.randint(
            0, 7200
        )  # Assume order payments are generally approved within 2 hours of placing order
        order_approved_timestamp = order_purchase_timestamp + timedelta(
            seconds=random_seconds_for_approval
        )

        # Assume orders are still being processed (not delivered to carrier and customer)
        order_delivered_carrier_date = order_delivered_customer_date = None

        # 50% orders were delivered between 6 to 16 days after order approval
        random_seconds_for_est_delivery = random.randint(
            6 * 24 * 60 * 60, 16 * 24 * 60 * 60
        )
        est_delivery_timestamp = order_approved_timestamp + timedelta(
            seconds=random_seconds_for_est_delivery
        )

        cursor.execute(
            """
            INSERT INTO orders (
                order_id, 
                customer_id, 
                order_status, 
                order_purchase_timestamp, 
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

    """,
            (
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_timestamp,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                est_delivery_timestamp,
            ),
        )

        total_price = 0
        # Simulate data for order_items table
        for idx in range(len(selected_product_ids)):
            order_item_id = idx + 1
            product_id = selected_product_ids[idx]
            seller_id = selected_seller_ids[idx]

            # 50% orders had shipping limit date between 4 to 6 days after order approval
            random_seconds_for_ship_limit = random.randint(
                4 * 24 * 60 * 60, 6 * 24 * 60 * 60
            )
            shipping_limit_timestamp = order_approved_timestamp + timedelta(
                seconds=random_seconds_for_ship_limit
            )

            product_price = random.uniform(
                product_dict[product_id]["min_price"],
                product_dict[product_id]["max_price"],
            )
            freight_value = random.uniform(
                product_dict[product_id]["min_freight"],
                product_dict[product_id]["max_freight"],
            )
            total_price += product_price
            total_price += freight_value

            if total_price == 0:
                print("Error encountered: total price is 0\n")
                print(f"Skipping transaction {i}. Continue to the next...")
                conn.rollback()
                skip_order = True
                break

            cursor.execute(
                """
                INSERT INTO order_items (
                        order_id,
                        order_item_id,
                        product_id,
                        seller_id,
                        shipping_limit_date,
                        price,
                        freight_value
                        )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    order_id,
                    order_item_id,
                    product_id,
                    seller_id,
                    shipping_limit_timestamp,
                    product_price,
                    freight_value,
                ),
            )
        if skip_order:
            continue

        # Simulate data for order_payments table
        payment_sequential = 1  # Assume only one credit card payment method is used
        payment_type = "credit_card"
        payment_installments = random.choices(
            [1, 2, 3, 4, 5, 6], weights=[62, 14, 11, 8, 6, 4]
        )[0]

        cursor.execute(
            """
            INSERT INTO order_payments (
                       order_id,
                       payment_sequential,
                       payment_type,
                       payment_installments,
                       payment_value
                       )
            VALUES (%s, %s, %s, %s, %s)    
""",
            (
                order_id,
                payment_sequential,
                payment_type,
                payment_installments,
                total_price,
            ),
        )
        conn.commit()
        success_counter += 1

except Exception as e:
    conn.rollback()
    print(f"Encountered error: {e}")
finally:
    print(f"Successfully committed {success_counter} transactions!")
    cursor.close()
    conn.close()
