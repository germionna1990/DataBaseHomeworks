from datetime import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


def check_analytics_files():
    files = [
        "/opt/airflow/dags/data/analytics_min_max_customers.csv",
        "/opt/airflow/dags/data/analytics_top5_wealth_segment.csv",
    ]

    for path in files:
        if not os.path.exists(path):
            raise ValueError("Ошибка")

        with open(path, "r") as f:
            lines = f.readlines()

        if len(lines) <= 1:
            raise ValueError("Ошибка")


with DAG(
    dag_id="load_csv_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["csv", "postgres", "analytics"],
) as dag:

    truncate_customer = PostgresOperator(
        task_id="truncate_customer",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE customer;",
    )

    copy_customer = PostgresOperator(
        task_id="copy_customer",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy customer
        FROM '/opt/airflow/dags/data/customer.csv'
        DELIMITER ';' CSV HEADER;
        """,
    )

    truncate_product = PostgresOperator(
        task_id="truncate_product",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE product;",
    )

    truncate_product_raw = PostgresOperator(
        task_id="truncate_product_raw",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE product_raw;",
    )

    copy_product_raw = PostgresOperator(
        task_id="copy_product_raw",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy product_raw
        FROM '/opt/airflow/dags/data/product.csv'
        DELIMITER ',' CSV HEADER;
        """,
    )

    insert_product = PostgresOperator(
        task_id="insert_product",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO product
        SELECT DISTINCT ON (product_id)
            product_id,
            brand,
            product_line,
            product_class,
            product_size,
            list_price,
            standard_cost
        FROM product_raw
        ORDER BY product_id;
        """,
    )

    truncate_orders = PostgresOperator(
        task_id="truncate_orders",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE orders;",
    )

    copy_orders = PostgresOperator(
        task_id="copy_orders",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy orders
        FROM '/opt/airflow/dags/data/orders.csv'
        DELIMITER ',' CSV HEADER;
        """,
    )

    truncate_order_items = PostgresOperator(
        task_id="truncate_order_items",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE order_items;",
    )

    copy_order_items = PostgresOperator(
        task_id="copy_order_items",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy order_items
        FROM '/opt/airflow/dags/data/order_items.csv'
        DELIMITER ',' CSV HEADER;
        """,
    )

    analytics_min_max = PostgresOperator(
        task_id="analytics_min_max_customers",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy (
            WITH customer_total AS (
                SELECT
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_amount
                FROM customer c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                LEFT JOIN order_items oi ON o.order_id = oi.order_id
                GROUP BY c.customer_id, c.first_name, c.last_name
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (ORDER BY total_amount ASC)  AS rn_min,
                       ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS rn_max
                FROM customer_total
            )
            SELECT *
            FROM ranked
            WHERE rn_min <= 3 OR rn_max <= 3
        )
        TO '/opt/airflow/dags/data/analytics_min_max_customers.csv'
        CSV HEADER;
        """,
    )

    analytics_top5_wealth = PostgresOperator(
        task_id="analytics_top5_wealth_segment",
        postgres_conn_id="postgres_default",
        sql="""
        \\copy (
            WITH customer_revenue AS (
                SELECT
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    c.wealth_segment,
                    COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) AS total_revenue
                FROM customer c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                LEFT JOIN order_items oi ON o.order_id = oi.order_id
                GROUP BY c.customer_id, c.first_name, c.last_name, c.wealth_segment
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY wealth_segment
                           ORDER BY total_revenue DESC
                       ) AS rn
                FROM customer_revenue
            )
            SELECT
                first_name,
                last_name,
                wealth_segment,
                total_revenue
            FROM ranked
            WHERE rn <= 5
        )
        TO '/opt/airflow/dags/data/analytics_top5_wealth_segment.csv'
        CSV HEADER;
        """,
    )

    check_analytics = PythonOperator(
        task_id="check_analytics_results",
        python_callable=check_analytics_files,
    )

    truncate_customer >> copy_customer

    truncate_product >> truncate_product_raw >> copy_product_raw >> insert_product

    truncate_orders >> copy_orders

    truncate_order_items >> copy_order_items

    [
        copy_customer,
        insert_product,
        copy_orders,
        copy_order_items,
    ] >> [
        analytics_min_max,
        analytics_top5_wealth,
    ] >> check_analytics
