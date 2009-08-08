#!/usr/bin/python

import sys
import musicsql.database

warn = sys.stderr.write


class Query(musicsql.Query):

    def __init__(self, **kargs):
        musicsql.Query.__init__(self, **kargs)
        self.requires = ()
            # list what other modules are required
        self.foreignkey = () # e.g., ['foreignkey', 'table']
            # add a foreign key to connect the new table to one of the
            # main hub tables (moments, notes, parts, noteheads, events)

    def set_properties(self):
        self.requires = []

    def sql(self, printing=False):
        import musicsql.alchemy as alchemy
        warn('Constructing query...\n')
        args = {database: self.dbName, backend: self.backend}
        part1 = alchemy.SQLsetup(**args)
        #
        # CONSTRUCT A QUERY HERE
        #
        sql = alchemy.assemble_query()
        return sql


class Query(musicsql.Function):

    def __init__(self, dbName):
        musicsql.Aggregate.__init__(self, dbName)
        self.requires = []
            # list what other modules are required
        self.foreignkey = [] # e.g., ['foreignkey', 'table']
            # add a foreign key to connect the new table to one of the
            # main hub tables (moments, notes, parts, noteheads, events)
        self.field_types = {} # e.g., {'note_id': 'INTEGER'} 
            # for the new fields created by this function, indicate
            # the type of data it contains (INTEGER, TEXT, FLOAT...)

    def function(self, row):
        '''
        function is run for each row of a result
        - return a list of fields corresponding to the 'newFields'
          established in the run function
        '''
        pass # e.g., self.add_output([column1, column2])

    def sql(self):
        import musicsql.alchemy as alchemy
        warn('Constructing query...\n')
        args = {database: self.dbName, backend: self.backend}
        part1 = alchemy.SQLsetup(**args)
        #
        # CONSTRUCT A QUERY HERE (see musicsql.alchemy)
        #
        sql = alchemy.assemble_query()
        return sql


class Query(musicsql.Aggregate):

    def __init__(self, dbName):
        musicsql.Aggregate.__init__(self, dbName)
        self.requires = []
            # list what other modules are required
        self.foreignkey = [] # e.g., ['foreignkey', 'table']
            # add a foreign key to connect the new table to one of the
            # main hub tables (moments, notes, parts, noteheads, events)
        self.groupFields = []
            # list of field names used to group aggregates 
        self.field_types = {} # e.g., {'note_id': 'INTEGER'} 
            # for the new fields created by this function, indicate
            # the type of data it contains (INTEGER, TEXT, FLOAT...)

    def init(self):
        '''
        init is run at the start of each new aggregate
        - set up the state variable
        '''
        pass  # e.g., self.state = {}

    def step(self, row):
        '''
        step is run for each row of an aggregate
        - update the state variable based on the row information
        - the input is a mapping between field labels and row data
        '''
        pass  # e.g., self.state['sum'] += row['field']

    def finalize(self):
        '''
        finalize is run at the end of each aggregate
        - return a list of fields corresponding to the 'newFields' and
          'outputFields' established in the run function
        '''
        pass # e.g., self.add_output([column1, column2])

    def sql(self):
        import musicsql.alchemy as alchemy
        warn('Constructing query...\n')
        args = {database: self.dbName, backend: self.backend}
        part1 = alchemy.SQLsetup(**args)
        #
        # CONSTRUCT A QUERY HERE
        #
        sql = alchemy.assemble_query()
        return sql


if __name__ == '__main__':
    import musicsql.options
    longOptions = ['database=', 'backend=', 'password?']
    options, args = musicsql.options.getOptions(longOptions)
    query = Query(tablename=None, **options)
    query.run(printing=True)
