from enum import Enum

from django.conf import settings
from django.db import models
from django.utils.translation import gettext_lazy as _

from dj_materialized_views.utils import (
    execute_raw_sql
)


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
        'dj_materialized_views.MaterializedView',
        related_name='indexes',
        on_delete=models.CASCADE
    )
    index_type = models.CharField(choices=IndexType.choices(), max_length=255, default=IndexType.BTREE.value)
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

        return execute_raw_sql(sql_command)

    def drop(self):
        """
        Drops the index from the materialized view table
        """
        db_table = self.materialized_view.db_table
        index_name = f'{db_table}_{self.index_field}'

        sql_command = f'DROP INDEX {index_name};'

        return execute_raw_sql(sql_command)
