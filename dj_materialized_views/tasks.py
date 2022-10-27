from celery import shared_task

from dj_materialized_views.apps import (
    MaterializedViewsAppConfig
)


@shared_task()
def refresh_materialized_view(materialized_view_id):
    """
    Task to periodically refresh the materialized view
    """
    from dj_materialized_views.models import MaterializedView

    materialized_view = MaterializedView.objects.get(id=materialized_view_id)
    materialized_view.refresh()


REFRESH_MV_TASK_FULL_NAME = f'{MaterializedViewsAppConfig.name}.tasks.{refresh_materialized_view.__name__}'
