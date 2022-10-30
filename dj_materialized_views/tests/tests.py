import json

from django.contrib.auth import get_user_model
from django.db import connection
from django.test.testcases import TestCase
from django.test.utils import CaptureQueriesContext

from django.contrib.admin import AdminSite
from django_celery_beat.models import PeriodicTask, IntervalSchedule

from dj_materialized_views.admin import MaterializedViewAdmin
from dj_materialized_views.models import MaterializedView, MaterializedViewIndex


class MockRequest(object):
    def __init__(self, user=None):
        self.user = user


class MaterializedViewTests(TestCase):
    def setUp(self):
        self.super_user = get_user_model().objects.create_superuser(username='s', email='s@e.or', password='p')

        # periodic refresh interval
        self.interval = IntervalSchedule.objects.create(every=1, period='hours')

    def _create_materialized_view(self, title, db_table='test', sql_query='SELECT * FROM django_migrations'):
        """
        Helper function that creates a materialized view table with unique index on the id column
        """
        periodic_task = PeriodicTask.objects.create(name='P', interval=self.interval)
        materialized_view_admin = MaterializedViewAdmin(model=MaterializedView, admin_site=AdminSite())

        materialized_view_data = dict(
            title=title,
            db_table=db_table,
            sql_query=sql_query,
            periodic_task=periodic_task
        )

        # When materialized view is created through the admin panel
        materialized_view_admin.save_model(
            obj=MaterializedView(**materialized_view_data),
            request=MockRequest(user=self.super_user),
            form=None, change=None
        )

        mv = MaterializedView.objects.get(title=title)
        MaterializedViewIndex.objects.create(
            title=f'{title}_idx',
            materialized_view=mv,
            index_field='id',
            is_unique=True
        )

        return mv

    def test__materialized_view__admin_creation(self):
        # WHEN materialized view is created from admin
        self._create_materialized_view(title='Test MV Creation')
        mv = MaterializedView.objects.get(title='Test MV Creation')

        # THEN
        # mv is created
        self.assertIsNotNone(mv.id)

        # periodic refresh should be enabled only after the mv is created in the db
        self.assertFalse(mv.periodic_task.enabled)

        # the mv is linked with the correct periodic refresh task through the id
        self.assertEquals(mv.periodic_task.task, 'dj_materialized_views.tasks.refresh_materialized_view')
        self.assertEquals(json.loads(mv.periodic_task.kwargs).get('materialized_view_id'), mv.id)

        # created by user is automatically set
        self.assertEquals(mv.created_by_user, self.super_user)

    def test__materialized_view__admin_action_create(self):
        # GIVEN materialized view is created from admin
        self._create_materialized_view(title='Test MV Admin Action Create')

        mv = MaterializedView.objects.get(title='Test MV Admin Action Create')

        # WHEN materialized view table is created in the database
        with CaptureQueriesContext(connection) as captured_queries:
            mv.create()

        mv.refresh_from_db()

        # THEN
        # the periodic refresh is enabled
        self.assertTrue(mv.periodic_task.enabled)

        # queries to create materialized view and indexes are executed
        queries = [q.get('sql') for q in captured_queries]
        create_mv_query = f'CREATE MATERIALIZED VIEW IF NOT EXISTS {mv.db_table} AS {mv.sql_query}'
        create_mv_index_query = f'CREATE UNIQUE INDEX IF NOT EXISTS test_id ON test USING btree(id);'

        self.assertIn(create_mv_query, queries)
        self.assertIn(create_mv_index_query, queries)

    def test__materialized_view__admin_action_refresh(self):
        # GIVEN materialized view is created from admin
        self._create_materialized_view(title='Test MV Admin Action Refresh')

        mv = MaterializedView.objects.get(title='Test MV Admin Action Refresh')

        mv.create()

        # WHEN materialized view table is dropped from the database
        with CaptureQueriesContext(connection) as captured_queries:
            mv.refresh()

        # THEN
        # query to refresh the materialized view is executed
        queries = [q.get('sql') for q in captured_queries]
        refresh_mv_query = f'REFRESH MATERIALIZED VIEW CONCURRENTLY {mv.db_table};'

        self.assertIn(refresh_mv_query, queries)

    def test__materialized_view__admin_action_drop(self):
        # GIVEN materialized view is created from admin
        self._create_materialized_view(title='Test MV Admin Action Drop')

        mv = MaterializedView.objects.get(title='Test MV Admin Action Drop')

        mv.create()
        # WHEN materialized view table is dropped from the database
        with CaptureQueriesContext(connection) as captured_queries:
            mv.drop()

        # THEN
        # periodic refresh is disabled
        self.assertFalse(mv.periodic_task.enabled)

        # query to drop materialized view is executed
        queries = [q.get('sql') for q in captured_queries]
        drop_mv_query = f'DROP MATERIALIZED VIEW IF EXISTS {mv.db_table};'

        self.assertIn(drop_mv_query, queries)
