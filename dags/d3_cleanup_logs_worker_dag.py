import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="D3_Cleanup_LOGS_Worker",
    description="Cleanup Logs Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="30 12 * * *",
    #schedule_interval="0 3,11,19 * * *",
    schedule_interval="0 21 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_cleanup_logs_sub_worker():
    from IngestionEngine.workers.cleanup_logs_worker import CleanupLogsWorker

    worker = CleanupLogsWorker()
    list_of_log_items = worker.find_eligible_items()

    return [_.LogID for _ in list_of_log_items]


@task(dag=dag)
def logs_cleanup_sub_worker(log_id):
    import logging

    logging.info("Cleanup logs Worker started its work.")
    from IngestionEngine.workers.cleanup_logs_worker import CleanupLogsWorker
    from IngestionEngine.models import IngestionLogs

    log_item = IngestionLogs.objects.get(LogID=log_id)
    worker = CleanupLogsWorker()
    worker.start(log_item)


f1 = find_file_for_cleanup_logs_sub_worker()

logs_cleanup_sub_worker.expand(log_id=f1)
