from django.contrib import messages
from django.db import transaction
from django.utils.translation import gettext_lazy as _


def create_materialized_view_action(description=_('Create Materialized View')):
    def create_materialized_view(model_admin, request, queryset):
        """
        Creates the materialized views and all the indexes associated with the view.
        If one of the SQL queries fails, the transaction is not committed
        """
        with transaction.atomic():
            for materialized_view in queryset:
                try:
                    materialized_view.create()
                except Exception as e:
                    return model_admin.message_user(request, f'Error: {e}', level=messages.ERROR)

        return model_admin.message_user(request, _('Materialized view created'))

    create_materialized_view.short_description = description

    return create_materialized_view


def refresh_materialized_view_action(description=_('Refresh Materialized View')):
    def refresh_materialized_view(model_admin, request, queryset):
        """
        Refreshes the materialized view.
        This automatically rebuilds the indexes
        """
        for materialized_view in queryset:
            try:
                materialized_view.refresh()
            except Exception as e:
                return model_admin.message_user(request, f'Error: {e}', level=messages.ERROR)

        return model_admin.message_user(request, _('Materialized view refreshed'))

    refresh_materialized_view.short_description = description

    return refresh_materialized_view


def drop_materialized_view_action(description=_('Drop Materialized View')):
    def drop_materialized_view(model_admin, request, queryset):
        """
        Drops tha materialized view table and removes the indexes
        """
        for materialized_view in queryset:
            try:
                materialized_view.drop()
            except Exception as e:
                return model_admin.message_user(request, f'Error: {e}', level=messages.ERROR)

        return model_admin.message_user(request, _('Materialized view dropped'))

    drop_materialized_view.short_description = description

    return drop_materialized_view


def create_index_action(description=_('Create Index')):
    def create_index_view(model_admin, request, queryset):
        for index in queryset:
            try:
                index.create()
            except Exception as e:
                return model_admin.message_user(request, f'Error: {e}', level=messages.ERROR)

        return model_admin.message_user(request, _('Index created'))

    create_index_view.short_description = description

    return create_index_view


def drop_index_action(description=_('Drop Index')):
    def drop_index_view(model_admin, request, queryset):
        for index in queryset:
            try:
                index.drop()
            except Exception as e:
                return model_admin.message_user(request, f'Error: {e}', level=messages.ERROR)

        return model_admin.message_user(request, _('Index dropped'))

    drop_index_view.short_description = description

    return drop_index_view
