import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(
    dag_id="C1_Clipper_Worker",
    description="Cliping Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="30 12 * * *",
    #schedule_interval="0 2,10,18 * * *",
    schedule_interval="5 3-14/2 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def find_file_for_clip_sub_worker():
    from IngestionEngine.workers.ctpi_clipper_worker import CTPIClipperWorker

    worker = CTPIClipperWorker()
    list_of_clip_requests = worker.find_eligible_items()

    return [_.ID for _ in list_of_clip_requests]


@task(dag=dag)
def clip_generator_sub_worker(clip_request_id):
    import logging

    logging.info("Clipper Worker started its work.")
    from IngestionEngine.workers.ctpi_clipper_worker import CTPIClipperWorker
    from SatProductCurator.models import NewClipRequest

    clip_request = NewClipRequest.objects.get(ID=clip_request_id)
    worker = CTPIClipperWorker()
    worker.start(clip_request)


f1 = find_file_for_clip_sub_worker()

clip_generator_sub_worker.expand(clip_request_id=f1)
