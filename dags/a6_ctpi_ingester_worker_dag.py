import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="A6_CTPI_Ingester_Worker",
    description="CTPI_Ingestor Worker Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="20 12 * * *",
    #schedule_interval="20 2,10,18 * * *",
    schedule_interval="55 3-14 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_ctpi_ingestor_sub_worker():
    from IngestionEngine.workers import CTPIIngesterWorker

    worker = CTPIIngesterWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_data]


@task(dag=dag)
def ctpi_ingestor_sub_worker(source_data_id):
    import logging

    logging.info("CTPI Ingester Worker started its work.")
    from IngestionEngine.workers import CTPIIngesterWorker
    from IngestionEngine.models import SourceData

    source_data = SourceData.objects.get(SourceDataID=source_data_id)
    worker = CTPIIngesterWorker()
    worker.start(source_data)


f1 = find_file_for_ctpi_ingestor_sub_worker()

ctpi_ingestor_sub_worker.expand(source_data_id=f1)
