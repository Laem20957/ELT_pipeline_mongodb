from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago

from elt_pipeline import main


def get_connection():
    hook = MongoHook(mongo_conn_id="mongo_conn")
    hook.get_conn()
    connect = hook.get_connection("mongo_conn")
    extra_args = connect.get_extra()
    port = connect.port
    login = connect.login
    password = connect.password
    database = extra_args.get("database")
    collection = extra_args.get("collection")
    return port, login, password, database, collection


get_conn = get_connection()

default_args = {
    "start_date": days_ago(1),
    "email": ["user_email"],
    "retries": 2,
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False
}


@dag(
    "DAG_elt_pipeline",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    description="DAG runs the script every 12 hours"
)
def dag_elt_pipeline():
    @task
    def task1():
        main(
            get_conn.port,
            get_conn.login,
            get_conn.password,
            get_conn.database,
            get_conn.collection
        )
    _ = task1()


dag_elt_pipeline = dag_elt_pipeline()
