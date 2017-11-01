
db_integration_test
===================

Easily setup relational database fixtures and assert database contents in unittest.


Install
-------

.. code-block:: bash

    $ pip install db_integration_test


Usage
-----

.. code-block:: python

    from dbit import DatabaseTestCase, fixture

    class SomeTest(DatabaseTestCase):

        def setUp(self):
            # the DB connection has to be created before the setUp is run.
            # DatabaseTestCase only ever creates a single connection. Multiple calls to connect are ignored.
            # to connect to a different DB, call self.disconnect(), then self.connect() with a different connection string.
            # uses SQLAlchemy under the hood
            self.connect('postgresql+psycopg2://user:pass@localhost/test_db')
            super().setUp()

        @fixture("table_name", [{'id': 1, 'value': 'abc'}, {'id': 2, 'value': 'def'}])
        def test_something_with_your_database(self):
            # interact with the DB
            # self.engine contains the SQLAlchemy Engine
            # self.session contains the SQLAlchemy ORM session
            # self.base contains the automap base for the current database
            # self.disconnect() disconnects from the DB

            # assert that the table contents match the fixture exactly
            self.assertMatchFixture("table_name", self.get_table_contents(self.base.classes.table_name))

            # assert that some number of rows appear in the fixture
            self.assertAllInFixture("table_name", [{'id': 1, 'value': 'abc'}])
