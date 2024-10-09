import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="D2_Cleanup_Clip_Images_Worker",
    description="Cleanup Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
   # schedule_interval="30 12 * * *",
    #schedule_interval="30 4,12,20 * * *",
    schedule_interval="0 23 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_cleanup_clip_images_sub_worker():
    from IngestionEngine.workers.cleanup_clip_images_worker import (
        CleanupClipImagesWorker,
    )

    worker = CleanupClipImagesWorker()
    list_of_clip_requests = worker.find_eligible_items()

    return [_.ID for _ in list_of_clip_requests]


@task(dag=dag)
def clip_cleanup_sub_worker(clip_request_id):
    import logging

    logging.info("Cleanup Clip Images Worker started its work.")
    from IngestionEngine.workers.cleanup_clip_images_worker import (
        CleanupClipImagesWorker,
    )
    from SatProductCurator.models import NewClipRequest

    clip_request = NewClipRequest.objects.get(ID=clip_request_id)
    worker = CleanupClipImagesWorker()
    worker.start(clip_request)


f1 = find_file_for_cleanup_clip_images_sub_worker()

clip_cleanup_sub_worker.expand(clip_request_id=f1)
