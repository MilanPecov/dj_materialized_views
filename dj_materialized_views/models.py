import json
from enum import Enum

from django.conf import settings
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils.translation import gettext_lazy as _
from django_celery_beat.models import PeriodicTask

from dj_materialized_views import tasks
from dj_materialized_views.utils import (
    run_custom_sql
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
        Used to disable the periodic task that refreshes the materialized view
        """
        if not self.periodic_task.enabled:
            self.periodic_task.enabled = True
            self.periodic_task.save()

    def disable_periodic_refresh(self):
        """
        Used to enable the periodic task that refreshes the materialized view
        """
        if self.periodic_task.enabled:
            self.periodic_task.enabled = False
            self.periodic_task.save()

    def create(self):
        """
        Creates a new materialized view table
        """
        with transaction.atomic():
            sql_command = f'CREATE MATERIALIZED VIEW IF NOT EXISTS {self.db_table} AS '
            sql_command += self.sql_query

            run_custom_sql(sql_command)

            self.enable_periodic_refresh()

    def refresh(self):
        """
        Concurrently refreshes the materialized view table
        """
        sql_command = f'REFRESH MATERIALIZED VIEW CONCURRENTLY {self.db_table};'

        return run_custom_sql(sql_command)

    def drop(self):
        """
        Drops the materialized view table
        """
        with transaction.atomic():
            sql_command = f'DROP MATERIALIZED VIEW IF EXISTS {self.db_table};'

            run_custom_sql(sql_command)
            self.disable_periodic_refresh()

    def link_periodic_refresh_task(self):
        """
        Links the periodic task that refreshes the materialized view
        with the particular materialized view instance during post_save
        """
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
def link_periodic_refresh_task_receiver(sender, instance, **kwargs):
    instance.link_periodic_refresh_task()


class MaterializedViewIndex(models.Model):
    """
    Used for adding indexes on the materialized view table
    """

    class IndexType(Enum):
        BTREE = "btree"
        GIN = "gin"
        GIST = "gist"
        HASH = "hash"

        @classmethod
        def choices(cls):
            return tuple((i.name, i.value) for i in cls)

    title = models.CharField(max_length=255)
    materialized_view = models.ForeignKey(
        MaterializedView,
        related_name='indexes',
        on_delete=models.CASCADE
    )
    index_type = models.CharField(choices=IndexType.choices(), max_length=255, default=IndexType.BTREE)
    index_field = models.CharField(max_length=255, help_text='DB field to be indexed')
    is_unique = models.BooleanField(default=False)

    created_by_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True, blank=True, editable=False,
        on_delete=models.SET_NULL
    )
    created_at = models.DateTimeField(auto_now_add=True)
    last_run_date = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _('Materialized View Index')
        verbose_name_plural = _('Materialized View Indexes')

    def __str__(self):
        return self.title

    def create(self):
        """
        Creates an index for the materialized view table
        """
        unique_index = 'UNIQUE' if self.is_unique else ''  # UNIQUE prefix
        db_table = self.materialized_view.db_table  # linked with the materialized view table
        index_name = f'{db_table}_{self.index_field}'

        sql_command = f'CREATE {unique_index} INDEX IF NOT EXISTS {index_name} ' \
                      f'ON {db_table} USING {self.index_type}({self.index_field});'

        return run_custom_sql(sql_command)

    def drop(self):
        """
        Drops the index from the materialized view table
        """
        db_table = self.materialized_view.db_table
        index_name = f'{db_table}_{self.index_field}'

        sql_command = f'DROP INDEX {index_name};'

        return run_custom_sql(sql_command)
