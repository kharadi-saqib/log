import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="B1_Sat_Product_Search_Worker",
    description="Satellite Product Search Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
  #  schedule_interval="30 12 * * *",
    #schedule_interval="30 2,10,18 * * *",
    schedule_interval="0 3-14/2 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_sat_search_sub_worker():
    from SatProductCurator.workers import SatProductSearchWorker

    worker = SatProductSearchWorker()
    collection_requests_ids = worker.find_eligible_items()

    return [_.ID for _ in collection_requests_ids]


@task(dag=dag)
def sat_search_sub_worker(new_collection_data_id):
    import logging

    logging.info("Satellite Product Search worker started its work.")
    from SatProductCurator.workers import SatProductSearchWorker
    from SatProductCurator.models import NewCollectionRequest

    new_collection_request = NewCollectionRequest.objects.get(ID=new_collection_data_id)
    worker = SatProductSearchWorker()
    worker.start(new_collection_request)


f1 = find_file_for_sat_search_sub_worker()

sat_search_sub_worker.expand(new_collection_data_id=f1)
