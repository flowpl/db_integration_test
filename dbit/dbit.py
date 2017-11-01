import json
import unittest

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

__base = None
__engine = None


make_session = sessionmaker()


def connect(connection_string):
    """
    :param connection_string: string
    :return:
    """
    global __base, __engine
    if not __base:
        __engine = create_engine(connection_string, echo=False)
        __base = automap_base()
        __base.prepare(__engine, reflect=True)
    return __engine, __base


class DbFixtureError(BaseException):
    def __init__(self, message, *args):
        super().__init__(*args)
        self.__message = message

    @property
    def message(self):
        return self.__message


def fixture(table_name, fixture_rows):
    """
    :param table_name: str the table to insert into
    :param fixture_rows: dict[] a list of rows to insert
    :return:
    """
    def fixture_decorator(func):
        def get_table(base, tname):
            for name, table in base.metadata.tables.items():
                if name == tname:
                    return table

        def execute(self, *args, **kwargs):
            # collect all the fixtures
            self.fixture_order.append(table_name)
            self.fixtures[table_name] = fixture_rows

            # if the next function called is not a fixture decorator
            if 'fixture_decorator' not in str(func):
                try:
                    # truncate all the tables used
                    for tn in self.fixtures:
                        self.session.execute('TRUNCATE TABLE "{}" CASCADE;'.format(tn))
                    self.session.commit()
                except AttributeError:
                    raise DbFixtureError('Not connected to Database. Call self.connect() before super().setUp() to fix.')
                except ProgrammingError:
                    raise DbFixtureError('Table "{}" does not exist.'.format(tn))

                # populate all the tables used
                for tn in self.fixture_order:
                    table = get_table(self.base, tn)
                    for row in self.fixtures[tn]:
                        self.session.execute(table.insert().values(**row))
                self.session.commit()

            func(self, *args, **kwargs)
        return execute
    return fixture_decorator


class DatabaseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__session = None
        self.__engine = None
        self.__base = None
        self.fixtures = {}
        self.fixture_order = []

    def connect(self, connection_string):
        """
        :param connection_string: string SQLAlchemy connection string http://docs.sqlalchemy.org/en/latest/core/engines.html
        :return:
        """
        self.__engine, self.__base = connect(connection_string)
        make_session.configure(bind=self.__engine)

    def create_session(self):
        if self.engine:
            self.__session = make_session()

    def close_session(self):
        if self.session:
            self.session.rollback()
            self.session.close()
            self.__session = None

    def disconnect(self):
        self.close_session()
        self.__engine = None
        self.__base = None

    @property
    def engine(self):
        """
        :return: sqlalchemy.engine.Engine
        """
        return self.__engine

    @property
    def base(self):
        """
        :return: sqlalchemy.ext.declarative.api.DeclarativeMeta
        """
        return self.__base

    @property
    def session(self):
        """
        :return: sqlalchemy.orm.Session
        """
        return self.__session

    def assertMatchFixture(self, table_name, example):
        try:
            fix = self.fixtures[table_name]
        except KeyError:
            raise DbFixtureError(
                'Fixture "{}" does not exist. ' +
                'Define it using the @fixture decorator on you test method.'.format(table_name))

        if not isinstance(example, (list, tuple)):
            raise ValueError('example must be a list of dicts')

        if len(fix) != len(example):
            raise AssertionError("expected result to have length {}, but it is {}".format(len(fix), len(example)))
        for i in range(len(fix)):
            self.assertDictEqual(fix[i], example[i])

    def assertAllInFixture(self, table_name, example):
        try:
            test_fixture = self.fixtures[table_name]
        except KeyError:
            raise DbFixtureError(
                'Fixture "{}" does not exist. ' +
                'Define it using the @fixture decorator on you test method.'.format(table_name))

        if not isinstance(example, (list, tuple)):
            raise ValueError('example must be a list of dicts')

        for row in example:
            fixture_found = False
            for fix in test_fixture:
                try:
                    self.assertDictEqual(row, fix)
                    fixture_found = True
                    break
                except AssertionError:
                    pass
            if not fixture_found:
                raise AssertionError('no exact match found in {} for {}'.format(table_name, json.dumps(row)))

    def get_table_contents(self, table):
        return [
            {col: getattr(row, col) for col in row.__table__.columns.keys()}
            for row
            in self.session.query(table).all()
        ]

    def setUp(self):
        super().setUp()
        self.create_session()

    def tearDown(self):
        super().tearDown()
        self.close_session()
