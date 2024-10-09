import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="E1_Find_Failed_Item_Worker",
    description="Find failed item Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="30 12 * * *",
    #schedule_interval="45 4,12,20 * * *",
    schedule_interval="* */6 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_failed_item_sub_worker():
    from IngestionEngine.workers import FindFailedItemWorker

    worker = FindFailedItemWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_data]


@task(dag=dag)
def find_failed_item_sub_worker(source_data_id):
    import logging

    logging.info("Find Failed Item Worker started its work.")
    from IngestionEngine.workers import FindFailedItemWorker
    from IngestionEngine.models import SourceData

    source_data = SourceData.objects.get(SourceDataID=source_data_id)
    worker = FindFailedItemWorker()
    worker.start(source_data)


f1 = find_file_for_failed_item_sub_worker()

find_failed_item_sub_worker.expand(source_data_id=f1)
