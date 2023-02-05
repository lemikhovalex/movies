from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from airflow import models

with models.DAG(
    dag_id="movies_etl_pg_to_es",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["etl"],
) as dag:

    fill_fw_es = BashOperator(
        task_id="fill_es_fw_from_q",
        bash_command=" ".join(
            [
                "cd /srv/app/src/etl/ && python cli.py fill-es-from-q",
                "--queue=fw --batch-size=256",
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
                    f"--queue-name={queue_name}",
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
                        f"--source-queue={queue_name} --target-queue=fw",
                    ]
                ),
                dag=dag,
            )
        else:
            fill_fw_q_task = BashOperator(
                task_id="empty-extract-fw-base-idx",
                bash_command="echo 1",
                dag=dag,
            )
        fill_base_task >> fill_fw_q_task >> fill_fw_es
