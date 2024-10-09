import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="C2_Clip_Ingester_Worker",
    description="Cliping Ingestion Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="30 12 * * *",
    #schedule_interval="35 2,10,18 * * *",
    schedule_interval="45 3-14/3 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_clip_ingestion_sub_worker():
    from IngestionEngine.workers.clip_ingester_worker import ClipIngesterWorker

    worker = ClipIngesterWorker()
    list_of_clip_requests = worker.find_eligible_items()

    return [_.ID for _ in list_of_clip_requests]


@task(dag=dag)
def clip_ingester_sub_worker(clip_request_id):
    import logging

    logging.info("Clip Ingestion Worker started its work.")
    from IngestionEngine.workers.clip_ingester_worker import ClipIngesterWorker
    from SatProductCurator.models import NewClipRequest

    clip_request = NewClipRequest.objects.get(ID=clip_request_id)
    worker = ClipIngesterWorker()
    worker.start(clip_request)


f1 = find_file_for_clip_ingestion_sub_worker()

clip_ingester_sub_worker.expand(clip_request_id=f1)
