import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="B2_Sat_Download_Worker",
    description="Satellite Download Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
  #  schedule_interval="30 12 * * *",
    #schedule_interval="40 2,10,18 * * *",
    schedule_interval="0 15-23/2 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_sat_download_sub_worker():
    from SatProductCurator.workers import SatDownloadWorker

    worker = SatDownloadWorker()
    collection_requests_ids = worker.find_eligible_items()

    return [_.id for _ in collection_requests_ids]


@task(dag=dag)
def sat_download_sub_worker(sat_image_tile_id):
    import logging

    logging.info("Satellite Download worker started its work.")
    from SatProductCurator.workers import SatDownloadWorker
    from SatProductCurator.models import SatelliteImageTile

    sat_image_tile = SatelliteImageTile.objects.get(id=sat_image_tile_id)
    worker = SatDownloadWorker()
    worker.start(sat_image_tile)


f1 = find_file_for_sat_download_sub_worker()

sat_download_sub_worker.expand(sat_image_tile_id=f1)
