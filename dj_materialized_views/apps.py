from django.apps import AppConfig as BaseConfig
from django.utils.translation import gettext_lazy as _


class MaterializedViewsAppConfig(BaseConfig):
    name = 'dj_materialized_views'
    verbose_name = _('Django Materialized Views')
    default_auto_field = 'django.db.models.AutoField'
