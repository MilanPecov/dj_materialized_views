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