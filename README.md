# Django Materialized Views

![workflow](https://github.com/MilanPecov/dj_materialized_views/actions/workflows/django.yml/badge.svg)

# Overview

Django Materialized Views is a powerful admin app that allows you to manage materialized views from the admin panel

The app can:

* Create, update and delete materialized views
* Create indexes for the materialized views
* Refresh the materialized views at regular intervals

Limitation:
* Works only with PostgreSQL

# Requirements
* Python 3.6+
* Django 2, 3 or 4
* Celery


# Installation
Install using pip
```
pip install dj_materialized_views 
```

Add 'django_celery_beat' and 'dj_materialized_views' to your INSTALLED_APPS setting
```
INSTALLED_APPS = [
    ...
    'django_celery_beat',
    'dj_materialized_views',
]
```

# Documentation

For more information on how to use the app, see the documentation at:

https://dj-materialized-views.readthedocs.io/en/latest/quick_start/