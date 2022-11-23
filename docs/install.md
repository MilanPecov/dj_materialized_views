## Install

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

Run migrations
```
python manage.py migrate
```

## Start Celery
Start the Celery Worker and the Celery Beat

```
# carries out the operations that update the materialized view.

celery -A tasks worker -l info
```

```
# produces the tasks for refreshing the materialized views

celery -A tasks beat -l info
```
