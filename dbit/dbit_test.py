from os import environ

from dbit import DbFixtureError
from . import DatabaseTestCase, fixture


TEST_TABLE_NAME = 'dbit_test'


class RegularTestMethodTest(DatabaseTestCase):
    def test_regular_test_method_executes_without_db(self):
        self.assertEqual(1, 1)


class NotConnectedTest(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        self.disconnect()

    def test_raise_exception_when_using_fixture_without_connect(self):
        @fixture("some_table", [])
        def test_method(me):
            pass

        with self.assertRaises(DbFixtureError) as context:
            test_method(self)

        self.assertIn('Not connected', context.exception.message)


class AutoConnectDatabaseTestCase(DatabaseTestCase):

    def setUp(self):
        self.connect(environ['TEST_DB_URL'])
        super().setUp()
        conn = self.engine.connect()
        conn.execute(
            'CREATE TABLE IF NOT EXISTS "{}" (id int primary key, value varchar(12));'
            .format(TEST_TABLE_NAME))
        conn.close()


class FixtureTest(AutoConnectDatabaseTestCase):
    def test_raise_error_when_adding_a_fixture_on_a_missing_table(self):
        with self.assertRaises(DbFixtureError) as context:
            @fixture("non_existing_table", [])
            def test_method(me):
                pass
            test_method(self)
        self.assertIn('does not exist', context.exception.message)


class AssertMatchFixtureTest(AutoConnectDatabaseTestCase):
    def test_raise_exception_when_missing_fixture(self):
        with self.assertRaises(DbFixtureError) as context:
            self.assertMatchFixture("some_table", [])

        self.assertIn('Define it using the @fixture decorator', context.exception.message)

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_raise_error_if_example_is_not_a_list(self):
        with self.assertRaises(ValueError) as context:
            self.assertMatchFixture(TEST_TABLE_NAME, {})
        self.assertIn('must be a list', str(context.exception))

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_assert_successfully_if_example_matches_fixture_exactly(self):
        self.assertMatchFixture(TEST_TABLE_NAME, self.get_table_contents(self.base.classes.dbit_test))

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_fail_assert_if_example_has_different_length_than_fixture(self):
        with self.assertRaises(AssertionError) as context:
            self.assertMatchFixture(TEST_TABLE_NAME, [{'id': 3, 'value': 6789}])
        self.assertIn('length 2, but it is 1', str(context.exception))

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_fail_assert_if_example_has_different_values_than_fixture(self):
        with self.assertRaises(AssertionError) as context:
            self.assertMatchFixture(TEST_TABLE_NAME, [{'id': 3, 'value': 6789}, {'id': 4, 'value': True}])
        self.assertIn(' != ', str(context.exception))

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_fail_assert_if_example_has_a_single_different_value_than_fixture(self):
        with self.assertRaises(AssertionError) as context:
            self.assertMatchFixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'deg'}])
        self.assertIn(' != ', str(context.exception))


class AssertAllInFixtureTest(AutoConnectDatabaseTestCase):
    def test_raise_exception_when_missing_fixture(self):
        with self.assertRaises(DbFixtureError) as context:
            self.assertAllInFixture("some_table", [])

        self.assertIn('Define it using the @fixture decorator', context.exception.message)

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_raise_error_if_example_is_not_a_list(self):
        with self.assertRaises(ValueError) as context:
            self.assertAllInFixture(TEST_TABLE_NAME, {})
        self.assertIn('must be a list', str(context.exception))

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_assert_successfully_if_all_rows_from_example_appear_exactly_in_fixture(self):
        self.assertAllInFixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}])

    @fixture(TEST_TABLE_NAME, [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
    def test_fail_assert_if_example_does_not_appear_in_fixture(self):
        with self.assertRaises(AssertionError) as context:
            self.assertAllInFixture(TEST_TABLE_NAME, [{'id': 3, 'value': 6789}])
        self.assertIn('no exact match found', str(context.exception))
