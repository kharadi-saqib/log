import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG
from datetime import timedelta

dag = DjangoDAG(dag_id="A4_COG_Generator_Worker",
                description='COG Generator Service',
                start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
              #  schedule_interval='30 12 * * *',
                #schedule_interval='0 1,9,17 * * *',
                schedule_interval='20 3-14 * * *',
                catchup=False,
                tags=['Worker']
)

dag.dag_run_timeout = timedelta(hours=6)

@task(dag=dag)
def find_file_for_cog_generator_sub_worker():

    from IngestionEngine.workers import CogGeneratorWorker

    worker = CogGeneratorWorker()
    list_of_source_data = worker.find_eligible_items()

    return [_.SourceDataID for _ in list_of_source_data]


@task(execution_timeout=timedelta(hours=6), dag=dag)
def cog_generator_sub_worker(source_data_id):

    import logging
    logging.info("Cog-Generator worker started its work.")
    from IngestionEngine.workers import CogGeneratorWorker
    from IngestionEngine.models import SourceData

    source_data = SourceData.objects.get(SourceDataID=source_data_id)
    worker = CogGeneratorWorker()
    worker.start(source_data)


f1 = find_file_for_cog_generator_sub_worker()

cog_generator_sub_worker.expand(source_data_id=f1)
