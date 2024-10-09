import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="E3_Sat_Usage_Ingester_Worker",
    description="Sat Usage Details Ingestion Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
   # schedule_interval="30 12 * * *",
    schedule_interval="40 7,15,23 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_sat_usage_ingester_sub_worker():
    from SatProductCurator.workers.sat_usage_details_ingester import (
        SatUsageIngesterWorker,
    )

    worker = SatUsageIngesterWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SATUsageID for _ in list_of_source_data]


@task(dag=dag)
def sat_usage_ingester_sub_worker(source_data_id):
    import logging

    logging.info("Satellite Usage Ingester worker started its work.")
    from SatProductCurator.workers.sat_usage_details_ingester import (
        SatUsageIngesterWorker,
    )
    from SatProductCurator.models import SatelliteUsageDetails

    source_data = SatelliteUsageDetails.objects.get(SATUsageID=source_data_id)
    worker = SatUsageIngesterWorker()
    worker.start(source_data)


f1 = find_file_for_sat_usage_ingester_sub_worker()

sat_usage_ingester_sub_worker.expand(source_data_id=f1)
