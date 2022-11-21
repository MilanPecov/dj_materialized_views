# dj-materialized-views

Django Materialized Views is a powerful admin app that allows you to manage materialized views from the admin panel


The app can:

* Create, update and delete materialized views
* Create indexes for the materialized views
* Refresh the materialized views at regular intervals

## Requirements

* `Python 3`
* `Django > 1`
* `Celery` - To periodically refresh the materialized views

## Limitations

* Works only with PostgreSQL for now