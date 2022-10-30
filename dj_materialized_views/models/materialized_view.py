import json

from django.conf import settings
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from django_celery_beat.models import PeriodicTask

from dj_materialized_views import tasks
from dj_materialized_views.utils import (
    execute_raw_sql
)


class MaterializedView(models.Model):
    """
    Represents the materialized view to be created and refreshed periodically.
    For creating a periodic background task to refresh the view this code relies on the
    django_celery_beat library.
    The materialized view can be queried through the Django ORM.
    """
    title = models.CharField(max_length=255)
    db_table = models.CharField(max_length=255, help_text=_('Name of the Materialized View table'))
    sql_query = models.TextField(help_text=_('SQL query to be materialize'))
    periodic_task = models.OneToOneField(
        PeriodicTask,
        on_delete=models.CASCADE
    )
    created_by_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True, editable=False,
        on_delete=models.SET_NULL
    )
    created_at = models.DateTimeField(auto_now_add=True)
    last_run_date = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _('Materialized View')
        verbose_name_plural = _('Materialized Views')

    def __str__(self):
        return self.title

    def delete(self, using=None, keep_parents=False):
        """
        Deletes the materialized view and the periodic task
        """
        with transaction.atomic():
            self.drop()  # remove from database
            self.periodic_task.delete()
            super(MaterializedView, self).delete()

    def enable_periodic_refresh(self):
        """
        Used to enable the periodic task that refreshes the materialized view
        """
        if not self.periodic_task.enabled:
            self.periodic_task.enabled = True
            self.periodic_task.save()

    def disable_periodic_refresh(self):
        """
        Used to disable the periodic task that refreshes the materialized view
        """
        if self.periodic_task.enabled:
            self.periodic_task.enabled = False
            self.periodic_task.save()

    def create(self):
        """
        Creates a new materialized view table with indexes
        """
        with transaction.atomic():
            sql_command = f'CREATE MATERIALIZED VIEW IF NOT EXISTS {self.db_table} AS '
            sql_command += self.sql_query

            execute_raw_sql(sql_command)

            for index in self.indexes.all():
                index.create()

            self.enable_periodic_refresh()

    def refresh(self):
        """
        Concurrently refreshes the materialized view table
        """
        with transaction.atomic():
            sql_command = f'REFRESH MATERIALIZED VIEW CONCURRENTLY {self.db_table};'

            execute_raw_sql(sql_command)

    def drop(self):
        """
        Drops the materialized view table
        """
        with transaction.atomic():
            sql_command = f'DROP MATERIALIZED VIEW IF EXISTS {self.db_table};'

            execute_raw_sql(sql_command)
            self.disable_periodic_refresh()

    def link_periodic_refresh_task(self):
        """
        Links the periodic task that refreshes the materialized view
        with the particular materialized view instance during post_save
        """
        if self.periodic_task.task != tasks.REFRESH_MV_TASK_FULL_NAME:
            self.periodic_task.task = tasks.REFRESH_MV_TASK_FULL_NAME  # connect the custom celery task
            self.periodic_task.kwargs = json.dumps({'materialized_view_id': self.pk})  # call the task with id param
            self.periodic_task.save()

    @property
    def model(self):
        """
        Returns unmanaged Django model (excluded from migrations) that can be
        used to run ORM queries against the materialized view table

        Example:

            my_materialized_view = MaterializedView.objects.first()
            my_materialized_view.model.objects.filter(my_field=my_value)
        """

        class MaterializedViewModel(models.Model):
            class Meta:
                managed = False
                db_table = self.db_table

        return MaterializedViewModel


@receiver(post_save, sender=MaterializedView)
def link_periodic_refresh_task_receiver(sender, instance, created, **kwargs):
    instance.link_periodic_refresh_task()

    if created:
        # do not enable the periodic refresh task when a mv is created
        # it should only be enabled after the table is created in the database
        instance.disable_periodic_refresh()
