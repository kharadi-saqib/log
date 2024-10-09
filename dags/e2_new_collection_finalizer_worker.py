""" This runs every 10 minutes. Avoid excessive logs in this worker. """

import pendulum
from datetime import timedelta
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="E2_NewCollection_Finalizer_Worker",
    description="Reports about new collections",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="15 4,12,20 * * *",
    schedule_interval="* 17 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def finalize_new_collection_requests():
    from SatProductCurator.workers import NewCollectionFinalizerWorker

    worker = NewCollectionFinalizerWorker()
    collection_requests = worker.find_eligible_items()

    for collection_request in collection_requests:
        worker.start(collection_request)


f1 = finalize_new_collection_requests()
