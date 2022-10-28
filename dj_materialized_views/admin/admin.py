from django.contrib import admin

from dj_materialized_views.admin.actions import create_materialized_view_action, refresh_materialized_view_action, \
    drop_materialized_view_action, create_index_action, drop_index_action
from dj_materialized_views.models import MaterializedView, MaterializedViewIndex


class MaterializedViewAdmin(admin.ModelAdmin):
    class MaterializedViewIndexInline(admin.TabularInline):
        model = MaterializedViewIndex
        fk_name = 'materialized_view'
        extra = 0  # do not show extra inline items

    list_display = ('title', 'db_table', 'created_by_user',)
    list_filter = ('title',)
    raw_id_fields = ('created_by_user',)
    inlines = [MaterializedViewIndexInline, ]

    actions = [
        create_materialized_view_action(),
        refresh_materialized_view_action(),
        drop_materialized_view_action()
    ]

    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by_user = request.user
        super().save_model(request, obj, form, change)

    def delete_queryset(self, request, queryset):
        for materialized_view in queryset:
            materialized_view.delete()


class MaterializedViewIndexAdmin(admin.ModelAdmin):
    list_display = ('title', 'created_by_user',)
    list_filter = ('title',)
    raw_id_fields = ('created_by_user',)

    actions = [
        create_index_action(),
        drop_index_action()
    ]

    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by_user = request.user
        super().save_model(request, obj, form, change)


admin.site.register(MaterializedView, MaterializedViewAdmin)
admin.site.register(MaterializedViewIndex, MaterializedViewIndexAdmin)
