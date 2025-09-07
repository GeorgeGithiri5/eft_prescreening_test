from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlalchemy


def ingest_data(**context):
    df = pd.read_csv("/opt/airflow/dags/data/transactions.csv")
    context['ti'].xcom_push(key="raw_data", value=df.to_dict(orient="records"))


def transform_data(**context):
    records = context['ti'].xcom_pull(key="raw_data", task_ids="ingest_data")
    df = pd.DataFrame(records)

    df = df.dropna()

    df["transaction_amount"] = df["transaction_amount"].astype(float)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    daily_agg = (
        df.groupby(["bank_id", df["transaction_date"].dt.date])
          .agg(total_volume=("transaction_amount", "sum"))
          .reset_index()
    )

    context['ti'].xcom_push(key="transformed_data", value=daily_agg.to_dict(orient="records"))


def load_raw_data(**context):
    """Load raw transactions into MySQL"""
    records = context['ti'].xcom_pull(key="raw_data", task_ids="ingest_data")
    df = pd.DataFrame(records)

    engine = sqlalchemy.create_engine(
        "mysql+pymysql://airflow:airflow@mysql:3306/airflow"
    )

    df.to_sql("raw_transactions", con=engine, if_exists="append", index=False)


def load_transformed_data(**context):
    """Load daily aggregated transactions into MySQL"""
    records = context['ti'].xcom_pull(key="transformed_data", task_ids="transform_data")
    df = pd.DataFrame(records)

    engine = sqlalchemy.create_engine(
        "mysql+pymysql://airflow:airflow@mysql:3306/airflow"
    )

    df.to_sql("daily_transaction_summary", con=engine, if_exists="append", index=False)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    "transaction_pipeline_dag",
    default_args=default_args,
    description="ETL pipeline for bank transactions",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
    )

    load_transformed_task = PythonOperator(
        task_id="load_transformed_data",
        python_callable=load_transformed_data,
    )

    # Dependencies
    ingest_task >> transform_task
    ingest_task >> load_raw_task
    transform_task >> load_transformed_task
