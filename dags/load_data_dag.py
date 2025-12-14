from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_customer():
    print("Загрузка customer.csv…")

def load_product():
    print("Загрузка product.csv…")

def load_orders():
    print("Загрузка orders.csv…")

def load_order_items():
    print("Загрузка order_items.csv…")

with DAG(
    dag_id="load_csv_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_customer = PythonOperator(
        task_id="load_customer",
        python_callable=load_customer
    )

    task_product = PythonOperator(
        task_id="load_product",
        python_callable=load_product
    )

    task_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders
    )

    task_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_order_items
    )

    task_customer >> task_product >> task_orders >> task_order_items
