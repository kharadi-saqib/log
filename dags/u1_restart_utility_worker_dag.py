import pendulum
from airflow.decorators import task
from django_dag import DjangoDAG
from datetime import timedelta
import pexpect
from django.conf import settings

dag = DjangoDAG(
    dag_id="U1_Restart_Utility_Worker",
    description="It Restarts File Watcher Worker Service",
    start_date=pendulum.datetime(2023, 1, 1, 5, 0, 0, tz="UTC"),
    #schedule_interval="0 3-15 * * *",
    schedule_interval="* 3-14/1 * * *",
    catchup=False,
    tags=["Worker"],
)


@task(dag=dag)
def restart_file_watcher_service():
    password = "!P@ssw0rd#"  # Replace with your actual sudo password
    command = "sudo systemctl restart file-watcher.service"

    try:
        # Use pexpect to handle the sudo password prompt
        child = pexpect.spawn(command)
        child.expect(".*password for.*:")
        child.sendline(password)
        child.expect(pexpect.EOF)

        return "Service restarted successfully"
    except pexpect.ExceptionPexpect as e:
        return f"Failed to restart service: {str(e)}"


# Define the task in the DAG
restart_service_task = restart_file_watcher_service()
