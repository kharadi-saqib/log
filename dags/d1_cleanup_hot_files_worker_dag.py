import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="D1_Cleanup_Hot_Files_Worker",
    description="Cleanup Hot Files Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
   # schedule_interval="30 12 * * *",
    #schedule_interval="30 3,11,19 * * *",
    schedule_interval="30 16 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_cleanup_hot_files_sub_worker():
    from IngestionEngine.workers.cleanup_hot_files_worker import CleanupHotFilesWorker

    worker = CleanupHotFilesWorker()
    list_of_source_datas = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_datas]


@task(dag=dag)
def hot_files_cleanup_sub_worker(source_data_id):
    import logging

    logging.info("Cleanup Hot Files Worker started its work.")
    from IngestionEngine.workers.cleanup_hot_files_worker import CleanupHotFilesWorker
    from IngestionEngine.models import SourceData

    source_data = SourceData.objects.get(SourceDataID=source_data_id)
    worker = CleanupHotFilesWorker()
    worker.start(source_data)


f1 = find_file_for_cleanup_hot_files_sub_worker()

hot_files_cleanup_sub_worker.expand(source_data_id=f1)
