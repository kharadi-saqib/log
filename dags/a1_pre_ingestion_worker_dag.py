import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="A1_Pre_Ingestion_Worker",
    description="Pre Ingestion Service",
    start_date=pendulum.datetime(2023, 1, 1, 13, 0, 0, tz="UTC"),
    #schedule_interval="0 12 * * *",
    #schedule_interval="0 0,8,16 * * *",
    schedule_interval="0 3-14 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_to_pre_ingest_sub_worker():
    from IngestionEngine.workers import PreIngestionWorker

    worker = PreIngestionWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_data]


@task(dag=dag)
def pre_ingestion_sub_worker(source_data_id):
    import logging

    logging.info("Pre-ingestion worker started its work.")
    from IngestionEngine.workers import PreIngestionWorker
    from IngestionEngine.models import SourceData

    hot_file = SourceData.objects.get(SourceDataID=source_data_id)
    worker = PreIngestionWorker()
    worker.start(hot_file)


f1 = find_file_to_pre_ingest_sub_worker()

pre_ingestion_sub_worker.expand(source_data_id=f1)
