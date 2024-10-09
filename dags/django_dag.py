
from airflow import DAG
from airflow.utils.decorators import apply_defaults


class DjangoDAG(DAG):
    @apply_defaults
    def __init__(self, *args, **kwargs):

        import os
        import django
        import sys

        sys.path.append(os.getcwd())
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "IADS.settings")
        django.setup()

        super().__init__(*args, **kwargs)
