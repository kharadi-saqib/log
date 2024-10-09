import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG


dag = DjangoDAG(dag_id="A3_Local_Ingestor_Worker",
                description='Local Ingestor Worker Service',
                start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
              #  schedule_interval='20 12 * * *',
                #schedule_interval='20 0,8,16 * * *',
                schedule_interval='10 3-14 * * *',
                catchup=False,
                tags=['Worker'])


@task(dag=dag)
def find_file_for_ingestor_sub_worker():

    from IngestionEngine.workers import LocalIngesterWorker

    worker = LocalIngesterWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_data]


@task(dag=dag)
def ingestor_sub_worker(source_data_id):

    import logging
    logging.info("Local Ingester worker started its work.")
    from IngestionEngine.workers import LocalIngesterWorker
    from IngestionEngine.models import SourceData

    source_data = SourceData.objects.get(SourceDataID=source_data_id)
    worker = LocalIngesterWorker()
    worker.start(source_data)


f1 = find_file_for_ingestor_sub_worker()

ingestor_sub_worker.expand(source_data_id=f1)
