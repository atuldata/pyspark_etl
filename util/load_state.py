"""
Use for managing the load state.
"""
from datetime import datetime

DELETE = '''
DELETE FROM load_state WHERE variable_name = ?'''

EXISTS = '''
SELECT 1 FROM load_state WHERE variable_name = ?'''

INSERT = '''
INSERT INTO
load_state(
    variable_value, variable_name, created_datetime, modified_datetime)
VALUES(?, ?, now(), now())'''

NEXTVAL = '''
SELECT NEXTVAL(?)
'''

SELECT = '''
SELECT variable_value, created_datetime, modified_datetime
FROM load_state
where variable_name = ?'''

UPDATE = '''
UPDATE load_state
SET variable_value = ?, modified_datetime = now()
WHERE variable_name = ?'''

ACCEPTABLE_DATE_STRINGS = [
    '%Y-%m-%d_%H %Z',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d'
]


def parse_date_string(date_string):
    """
    TODO: Use https://pypi.python.org/pypi/python-dateutil instead!
    """

    def _parse_date_string(_date_string, _date_format):
        try:
            return datetime.strptime(_date_string, _date_format)
        except:
            return None

    for _date_format in ACCEPTABLE_DATE_STRINGS:
        date_time = _parse_date_string(date_string, _date_format)
        if date_time is not None:
            return date_time
        continue
    return None


class LoadState(object):
    """
    For working with the load state.
    Requires valid oxdb object.
    variable_name can be set after instantiation.
    """
    _exists = None

    def __init__(self, oxdb, variable_name=None):
        self.oxdb = oxdb
        self.variable_name = variable_name
        self.variable_value = None
        self.created_datetime = None
        self.modified_datetime = None
        self.select()

    def delete(self):
        """
        Remove the varible_name from the load_state table.
        """
        self.oxdb.execute(DELETE, self.variable_name, commit=True)
        self._exists = None

    @property
    def exists(self):
        """
        Does the variable_name already exist in the load_state table?
        """
        if self._exists is None:
            self._exists = \
                bool(
                    self.oxdb.get_executed_cursor(
                        EXISTS, self.variable_name).fetchone())

        return self._exists

    def increment(self, commit=False):
        """
        Sets the value to the NEXTVAL for the variable_name.
        """
        self.upsert(
            self.oxdb.get_executed_cursor(
                NEXTVAL, '.'.join([self.oxdb.schema, self.variable_name])
            ).fetchone()[0], commit=commit)

    def select(self):
        """
        Sets local values to the db values.
        """
        row = \
            self.oxdb.get_executed_cursor(SELECT, self.variable_name).fetchone()
        if row is not None:
            (self.variable_value, self.created_datetime,
             self.modified_datetime) = [item for item in row]
            self._exists = True
        return row

    def update_variable_datetime(
            self, variable_value=None, commit=False, force=False):
        """
        Will only update the variable_value to a string representation of a
        datetime or parsable string passed in as variable_value.
        Variable value must be a datetime.datetime object.
        """
        if variable_value is not None:
            new_value = variable_value
            if isinstance(new_value, (str, unicode)):
                new_value = \
                    parse_date_string('%s UTC' % new_value) \
                    or parse_date_string(new_value)
            if new_value is not None:
                if not force and self.variable_value is not None:
                    current_value = parse_date_string(self.variable_value)
                    if current_value is not None and new_value < current_value:
                        return
                self.upsert(
                    new_value.strftime('%Y-%m-%d %H:%M:%S'), commit=commit)

    def upsert(self, variable_value=None, commit=False):
        """
        Update the load_state. Insert it if it does not exist.
        """
        statement = UPDATE if self.exists else INSERT
        self.oxdb.execute(
            statement,
            variable_value or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            self.variable_name, commit=commit)
        self.select()

    insert = upsert

    update = upsert
