"""
Django settings for IADS project.

Generated by 'django-admin startproject' using Django 4.2.4.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.2/ref/settings/
"""

import environ
import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

env = environ.Env()
environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-_6iid!nmm_25_e2!!7(p)s1_l)b37s0q^i=4u+1l*#q3)ukpfe"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env("DEBUG")
IS_STAGING = True if env("IS_STAGING") == "True" else False

ALLOWED_HOSTS = ["*"]

# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.gis",
    "rest_framework",
    "rest_framework.authtoken",
    "rest_framework_gis",
    "django_object_actions",
    "corsheaders",
    "IngestionEngine",
    "SatProductCurator",
    "MnaApp",
    "CostEstimator",
    "Processor",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "IngestionEngine.middleware.Custom404Middleware",
]

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.TokenAuthentication",
    ],
}


ROOT_URLCONF = "IADS.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            os.path.join(BASE_DIR, "templates"),
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "IADS.wsgi.application"


# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

DATABASES = {
    "default": {**env.db(), "ENGINE": "django.contrib.gis.db.backends.postgis", "CONN_MAX_AGE":21600},
}

# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"
STATIC_ROOT = "static/"
MEDIA_ROOT = "media/"
STATICFILES_DIRS = [
    BASE_DIR / "dist",
]

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

CORS_ALLOW_ALL_ORIGINS = True

_1mb = 1024 * 1024
LOG_DIR = "logs"
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"verbose": {"format": "[%(levelname)s] [%(asctime)s] %(message)s"}},
    "handlers": {
        "django": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "django.log"),
            "maxBytes": _1mb * 10,
            "backupCount": 5,
            "formatter": "verbose",
        },
        "ingestion-engine": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "ingestion-engine.log"),
            "maxBytes": _1mb * 10,
            "backupCount": 5,
            "formatter": "verbose",
        },
        "sat-curator": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "sat-curator.log"),
            "maxBytes": _1mb * 10,
            "backupCount": 5,
            "formatter": "verbose",
        },
        "mna-app": {
            "level": "DEBUG",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOG_DIR, "mna-app.log"),
            "maxBytes": _1mb * 10,
            "backupCount": 5,
            "formatter": "verbose",
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["django"],
            "level": "INFO",
        },
        "ingestion-engine": {
            "handlers": ["ingestion-engine", "console"],
            "level": "DEBUG",
        },
        "sat-curator": {
            "handlers": ["sat-curator"],
            "level": "INFO",
        },
        "mna-app": {
            "handlers": ["mna-app", "console"],
            "level": "DEBUG",
        },
    },
}

# Celery
CELERY_BROKER_URL = "redis://127.0.0.1:6379/0"
CELERY_RESULT_BACKEND = "redis://127.0.0.1:6379/0"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"

HOT_FOLDER = env("HOT_FOLDER")
if not os.path.exists(HOT_FOLDER):
    raise Exception("HOT_FOLDER not found")

LOCAL_INGESTED_FOLDER = env("LOCAL_INGESTED_FOLDER")
if not os.path.exists(LOCAL_INGESTED_FOLDER):
    raise Exception("LOCAL_INGESTED_FOLDER not found")

SAT_DOWNLOAD_FOLDER = env("SAT_DOWNLOAD_FOLDER")
if not os.path.exists(SAT_DOWNLOAD_FOLDER):
    raise Exception("SAT_DOWNLOAD_FOLDER not found")

CLIPPED_IMAGES_FOLDER = env("CLIPPED_IMAGES_FOLDER")
if not os.path.exists(CLIPPED_IMAGES_FOLDER):
    os.mkdir(CLIPPED_IMAGES_FOLDER)


FAIL_WORKER_AFTER_SECONDS = env.int("FAIL_WORKER_AFTER_SECONDS")

DELETE_CLIP_IMAGE_AFTER_DAYS = env.int("DELETE_CLIP_IMAGE_AFTER_DAYS")

DELETE_HOT_FOLDER_IMAGE_AFTER_DAYS = env.int("DELETE_HOT_FOLDER_IMAGE_AFTER_DAYS")

DELETE_LOGS_AFTER_DAYS = env.int("DELETE_LOGS_AFTER_DAYS")

# Check if FAIL_WORKER_AFTER_SECONDS is not set or is empty
if not FAIL_WORKER_AFTER_SECONDS:
    raise Exception("FAIL_WORKER_AFTER_SECONDS is not set or is empty.")

KEYCLOAK_TOKEN_URL = env("KEYCLOAK_TOKEN_URL")
if not (
    KEYCLOAK_TOKEN_URL.startswith("http://")
    or KEYCLOAK_TOKEN_URL.startswith("https://")
):
    raise Exception("Invalid Keycloak token URL")

SERVER_PATH = env("SERVER_PATH")

# if not os.path.exists(SERVER_PATH):
#   raise Exception("SERVER_PATH not found")

CLIP_SERVER_PATH = env("CLIP_SERVER_PATH")
if not os.path.exists(CLIP_SERVER_PATH):
    raise Exception("CLIP_SERVER_PATH not found")


MNA_METADATA_INSERT_URL = env("MNA_METADATA_INSERT_URL")
if not (
    MNA_METADATA_INSERT_URL.startswith("http://")
    or MNA_METADATA_INSERT_URL.startswith("https://")
):
    raise Exception("Invalid MNA_METADATA_INSERT_URL")


MNA_CLIP_INSERT_URL = env("MNA_CLIP_INSERT_URL")
if not (
    MNA_CLIP_INSERT_URL.startswith("http://")
    or MNA_CLIP_INSERT_URL.startswith("https://")
):
    raise Exception("Invalid MNA_CLIP_INSERT_URL")

SERVER_URL = env("SERVER_URL")
if not (SERVER_URL.startswith("http://") or SERVER_URL.startswith("https://")):
    raise Exception("Invalid SERVER_URL")

CLIP_SERVER_URL = env("CLIP_SERVER_URL")
if not (
    CLIP_SERVER_URL.startswith("http://") or CLIP_SERVER_URL.startswith("https://")
):
    raise Exception("Invalid CLIP_SERVER_URL")

SAT_INSERT_TOKEN_URL = env("SAT_INSERT_TOKEN_URL")
if not (
    SAT_INSERT_TOKEN_URL.startswith("http://")
    or SAT_INSERT_TOKEN_URL.startswith("https://")
):
    raise Exception("Invalid SAT_INSERT_TOKEN_URL")

MNA_SAT_DETAILS_INSERT_URL = env("MNA_SAT_DETAILS_INSERT_URL")
if not (
    MNA_SAT_DETAILS_INSERT_URL.startswith("http://")
    or MNA_SAT_DETAILS_INSERT_URL.startswith("https://")
):
    raise Exception("Invalid MNA_SAT_DETAILS_INSERT_URL")

MNA_GIVE_TILE_COUNT_URL = env("MNA_GIVE_TILE_COUNT_URL")
if not (
    MNA_GIVE_TILE_COUNT_URL.startswith("http://")
    or MNA_GIVE_TILE_COUNT_URL.startswith("https://")
):
    raise Exception("Invalid MNA_GIVE_TILE_COUNT_URL")

PENTA_GRANT_TYPE = env("PENTA_GRANT_TYPE")
PENTA_CLIENT_ID = env("PENTA_CLIENT_ID")
PENTA_CLIENT_SECRET = env("PENTA_CLIENT_SECRET")
PENTA_API_USERNAME = env("PENTA_API_USERNAME")
PENTA_AUTH_PASSWORD = env("PENTA_AUTH_PASSWORD")
PENTA_ORG_ID = env("PENTA_ORG_ID")
PENTA_USER_ROLE = env("PENTA_USER_ROLE")
SAT_CLIENT_SECRET = env("SAT_CLIENT_SECRET")
SAT_PASSWORD = env("SAT_PASSWORD")
CONTENT_TYPE = env("CONTENT_TYPE")
SAT_PENTA_ORG_ID = env("SAT_PENTA_ORG_ID")
PENTA_SELECTED_LOCALE = env("PENTA_SELECTED_LOCALE")
HOST_ID = env("HOST_ID")
PENTA_SELECTED_LOCALE_FOR_CLIPPING = env("PENTA_SELECTED_LOCALE_FOR_CLIPPING")
GENERATE_COG_ON_LOCAL = env("GENERATE_COG_ON_LOCAL")
TO_BE_DOWNLOADED = env("TO_BE_DOWNLOADED")
COPY_SCRIPT_PATH = env("COPY_SCRIPT_PATH")
CREAT_MONTH_FOLDER_ON_INGESTED_RESOURCE = env("CREAT_MONTH_FOLDER_ON_INGESTED_RESOURCE")

if PENTA_ORG_ID is None:
    raise ValueError("Penta Organization ID must be set in the environment.")
if PENTA_USER_ROLE is None:
    raise ValueError("Penta UserRole must be set in the environment.")
if PENTA_GRANT_TYPE is None:
    raise ValueError("PENTA_GRANT_TYPE must be set in the environment.")
if PENTA_CLIENT_ID is None:
    raise ValueError("PENTA_CLIENT_ID must be set in the environment.")
if PENTA_CLIENT_SECRET is None:
    raise ValueError("PENTA_CLIENT_SECRET must be set in the environment.")
if PENTA_API_USERNAME is None:
    raise ValueError("PENTA_API_USERNAME must be set in the environment.")
if PENTA_AUTH_PASSWORD is None:
    raise ValueError("PENTA_AUTH_PASSWORD must be set in the environment.")
if SAT_CLIENT_SECRET is None:
    raise ValueError("SAT_CLIENT_SECRET must be set in the environment.")
if SAT_PASSWORD is None:
    raise ValueError("SAT_PASSWORD must be set in the environment.")
if CONTENT_TYPE is None:
    raise ValueError("CONTENT_TYPE must be set in the environment.")
if HOST_ID is None:
    raise ValueError("HOST_ID must be set in the environment.")
if SAT_PENTA_ORG_ID is None:
    raise ValueError(
        "Penta Organization ID for sat ingester must be set in the environment."
    )
if PENTA_SELECTED_LOCALE is None:
    raise ValueError("PENTA_SELECTED_LOCALE must be set in the environment.")
if PENTA_SELECTED_LOCALE_FOR_CLIPPING is None:
    raise ValueError(
        "PENTA_SELECTED_LOCALE for clip insert api must be set in the environment."
    )

os.environ["EODAG__PEPS__DOWNLOAD__OUTPUTS_PREFIX"] = SAT_DOWNLOAD_FOLDER
os.environ["EODAG__PLANETARY_COMPUTER__DOWNLOAD__OUTPUTS_PREFIX"] = SAT_DOWNLOAD_FOLDER