import uuid

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import XComArg, models


def get_q_name_with_id(q: str, id_: str) -> str:
    out = f"q-{id_}"
    out = out.replace("-", "_")
    return out


def get_uuid(ti):
    ti.xcom_push(key="id", value=str(uuid.uuid4()))
    return


with models.DAG(
    dag_id="movies_etl_pg_to_es",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["etl"],
) as dag:

    init_node = PythonOperator(task_id="init_uuid_gen", python_callable=get_uuid)

    final_node = BashOperator(
        task_id="final-operator",
        bash_command="echo 0",
        dag=dag,
    )

    fill_fw_es = BashOperator(
        task_id="fill_es_fw_from_q",
        bash_command=" ".join(
            [
                "cd /srv/app/src/etl/ && python cli.py fill-es-from-q",
                f"--queue=fw-{XComArg(init_node, key='id')} --batch-size=256",
            ]
        ),
        dag=dag,
    )

    for (extracter, queue_name, batch_size,) in zip(
        ("PersonExtracter", "FilmworkExtracter", "GenreExtracter"),
        ("p", "fw", "g"),
        (5, 256, 2),
    ):
        fill_base_task = BashOperator(
            task_id=f"fill_base_q_for_{extracter}",
            bash_command=" ".join(
                [
                    "cd /srv/app/src/etl/ && python cli.py fill-base-q",
                    f"--extracter={extracter}  --batch-size={batch_size}",
                    f"--queue-name={queue_name}-{XComArg(init_node, key='id')}",
                ]
            ),
            dag=dag,
        )
        # clean_q = BashOperator(
        #     task_id=f"clean_{queue_name}_queue",
        #     bash_command="echo 0",
        #     dag=dag,
        # )
        clean_q = BashOperator(
            task_id=f"clean_{queue_name}_queue",
            bash_command=" ".join(
                [
                    "cd /srv/app/src/etl/ && python cli.py clean-q",
                    f"--queue={queue_name}-{XComArg(init_node, key='id')}",
                ]
            ),
            dag=dag,
        )

        if queue_name != "fw":
            fill_fw_q_task = BashOperator(
                task_id=f"fill_fw_queue_from_{queue_name}_queue",
                bash_command=" ".join(
                    [
                        "cd /srv/app/src/etl/ && python cli.py fill-q-from-q",
                        f"--extracter=FilmworkExtracter --batch-size={batch_size}",
                        f"--source-queue={queue_name}-{XComArg(init_node, key='id')}",
                        f"--target-queue=fw-{XComArg(init_node, key='id')}",
                    ]
                ),
                dag=dag,
            )
            (
                init_node
                >> fill_base_task
                >> fill_fw_q_task
                >> [fill_fw_es, clean_q]
                >> final_node
            )
        else:
            init_node >> fill_base_task >> fill_fw_es >> clean_q >> final_node
