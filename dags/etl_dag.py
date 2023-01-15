from airflow import models
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with models.DAG(
    dag_id="movies_etl_pg_to_es",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["etl"],
) as dag:

    fill_fw_es = BashOperator(
        task_id=f"fill_es_fw_from_q",
        bash_command=f"""cd /srv/src && python etl/cli.py fill_es_from_q --queue=fw --batch_size=512""",
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
                    "cd /srv/src && python etl/cli.py fill_base_q",
                    f"--extracter=FilmworkExtracter  --batch_size={batch_size}"
                    f"--queue_name={queue_name}",
                ]
            ),
            dag=dag,
        )

        if queue_name != "fw":
            fill_fw_q_task = BashOperator(
                task_id=f"fill_fw_queue_from_{queue_name}_queue",
                bash_command=" ".join(
                    [
                        "cd /srv/src && python etl/cli.py fill_q_from_q",
                        f"--extracter=FilmworkExtracter --batch_size={batch_size} --queue_name={queue_name}",
                    ]
                ),
                dag=dag,
            )
        else:
            fill_fw_q_task = BashOperator(
                task_id="empty_extract_fw_base_idx",
                bash_command="echo 1",
                dag=dag,
            )
        fill_base_task >> fill_fw_q_task >> fill_fw_es
