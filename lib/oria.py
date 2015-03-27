#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
load data into ORIA DB

Helper library for loading data from various sources into the ORIA
database.  Includes offline mockup for testing and development, and
command-line arguments for connection.

Written for the University of Illinois.

Example usage:

    # Parse user options.
    parser = oria.ArgumentParser(
        description="loads data into ORIA DB"
    )
    parser.add_argument( "file", type=open,
                         help="file to read" )
    args = parser.parse_args()

    # Pick which database to connect to.
    if args.db is None:
        args.db = oria.DB_BASE_TEST

    # Connect to the database.
    db = oria.DBConnection( db=args.db, offline=args.offline,
                            db_write=args.db_write, debug=args.debug )

    # Look up ID by key:
    db.start()
    db.fetch_id( table, key_name, key_value, key_value_cache )
    db.finish()

    # Assert a key-value pair in the database:
    db.get_or_set_id( key_value_cache, table, key_name,
                      column_values )

    # Read an expected single result:
    db.start()
    result_row = db.read( statement, params )
    db.finish()

    # Get possible multiple results:
    result_rows = db.read_many( statement, params )

    # Write to the database:
    db.start()
    db.write( statement, params )
    db.finish()
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"23 March 2015"
__version__ = 1.5

import argparse
from MySQLdb import connect

# DB access constants
DB_BASE = "oria_master"
DB_BASE_TEST = "oria_test"
DB_HOST = "localhost"
DB_PORT = 3306
DB_PASS = "justdroning"
DB_USER = "loader_bot"

# Result page size.
PAGESIZE = 10000

class ArgumentParser( argparse.ArgumentParser ):
    """
    Subclass the argument parser with options we will want for every
    data loader: offline, test, debug.
    """
    def __init__( self, *args, **kwargs ):
        """
        Predefine three command-line arguments for the parser.
        """
        # Pass the arguments on to our superclass.
        argparse.ArgumentParser.__init__( self, *args, **kwargs )

        # Add the options we always want.
        self.add_argument( "--offline", action="store_true",
                           help="do not try to connect to DB" )
        self.add_argument( "-t", "--test", action="store_true",
                           help="do not actually write changes to DB" )
        self.add_argument( "-d", "--debug", action="store_true",
                           help="explain everything going on" )
        self.add_argument( "--db", "--database",
                           choices=[ "master", "test" ],
                           help="database to write to" )
        self.add_argument( "--port", type=int, default=DB_PORT,
                           help="database server port number")
        return

    def parse_args( self ):
        """
        Parse the command-line arguments, and calculate some inferred
        values, so offline, test, debug, and db_write are available.
        """
        # Call the superclass for the literal arguments.
        args = argparse.ArgumentParser.parse_args( self )

        # But then do some logic to invent some additional useful
        # values.
        args.db_write = not args.offline and not args.test
        if args.db == "master":
            args.db = DB_BASE
        elif args.db == "test":
            args.db = DB_BASE_TEST

        return args

class DBConnection( object ):
    """
    Broker connections to the ORIA database, including faking it
    depending on test or offline status.  Defaults to production mode
    (live master database, writing, no debug).
    """
    def __init__( self, host=DB_HOST, port=DB_PORT, user=DB_USER,
                  passwd=DB_PASS, db=DB_BASE, offline=False,
                  db_write=True, debug=False ):
        """
        Start a connection to the database, if appropriate.  Fake it
        if not, to make writing easier.
        """
        self._db = None
        self._cursor = None
        self._offset = None

        self.offline = offline
        self._db_write = db_write
        self._debug = debug

        if not self.offline:
            self._db = connect( host=host, port=port,
                                user=user, passwd=passwd,
                                db=db )
        return

    def fetch_id( self, table_name, column_name, column_value,
                  cache=None, id="id" ):
        """
        Queries the database for a single column-value pair on the
        given table, and returns the value of the ID column.  If used
        when offline and without a cache, results are undefined.  If a
        cache is given, tries to use it, and updates it with the
        column_value as a key.  The ID column defaults to “id” but can
        be overridden.

        Returns None if no match is found.
        """
        # Try the cache first.
        if cache is not None and column_value in cache:
            return cache[column_value]

        # If we’re offline, delegate this to get_or_set_id; we don’t
        # care about any other columns.
        if self.offline:
            return self.get_or_set_id( cache,
                                       table_name,
                                       column_name,
                                       { column_name :
                                             column_value } )

        # Note double % here... we do formatted string substitution to put
        # the table and column name in first, then let MySQLdb handle the
        # value substitution.  Complicated!
        sql_stmt = "SELECT %s FROM %s WHERE %s = %%s;" % \
            ( id, table_name, column_name )

        result_row = self.read( sql_stmt, ( column_value, ), 1 )

        if result_row is None:
            return None
        else:
            if cache is not None:
                cache[column_value] = result_row[0]
            return result_row[0]

    def finish( self ):
        """
        End a transaction session; if offline, fake the commit.
        """
        if not self.offline and self._cursor is not None:
            self._db.commit()
            self._cursor.close()
            self._offset = None

        self._cursor = None

    def get_or_set_id( self, cache, table_name, key_name, columns,
                       id="id" ):
        """
        Look up an entity using the information in the dictionary.  If
        it exists, return its ID; if not, create it and return the
        resulting ID.  If offline or read-only, simulate the creation.
        Use the given cache.

        This only works when there is a unique code (such as a Banner
        ID) by which the item is looked up.  There may be arbitrary
        information to write.

        The key_name must be one of the column names.  The column
        structure is a dictionary of column names to values (to look
        up, as the key, and then to write, if the key is not found).
        The ID column defaults to “id.”
        """
        # Unpack the structured info.
        key_value = columns[ key_name ]

        # Check the cache for the code, first.
        if key_value in cache:
            if self._debug:
                print "*** Found %s in cache: %s\n" % ( key_name,
                                                        key_value )
            return cache[ key_value ]

        # Start a transaction session; be sure to finish it!
        self.start()

        # If not offline, check the database for a known entry.
        if not self.offline:
            item_id = self.fetch_id( table_name, key_name, key_value,
                                     id=id )

            # If found, update the cache and return.
            if item_id is not None:
                cache[ key_value ] = item_id
                return item_id

        # If read-only, fake a DB write and update the cache.
        if not self._db_write:
            if self._debug:
                print "*** Faking database write: setting " + \
                    "%s to %s in %s\n" % ( key_name, key_value,
                                           table_name )
            if "NEXT_ID" not in cache:
                cache[ "NEXT_ID" ] = 1
            cache[ key_value ] = cache[ "NEXT_ID" ]
            cache[ "NEXT_ID" ] += 1
            return cache[ key_value ]

        # Write to the database, update the cache, and return the ID.
        col_names = columns.keys()
        col_values = [ columns[name] for name in col_names ]

        sql_stmt = "INSERT INTO %s ( %s ) VALUES ( %s );" % \
            ( table_name,
              ", ".join( col_names ),
              ", ".join( [ "%s" for v in col_values ] ) )

        self.write( sql_stmt, col_values )

        item_id = self.fetch_id( table_name, key_name, key_value, id=id )

        # Actually make the changes we wanted!
        self.finish()

        # If found, update the cache and return.
        if item_id is not None:
            cache[ key_value ] = item_id
            return item_id

        # Something went badly wrong if we got past all that.
        raise ORIALookupError, \
            "Unable to find or create %s: %s (%s)" % \
            ( table_name, key_name, key_value )

    def read( self, statement, params, results=1 ):
        """
        Read a single row from the database, if online; fake it if not.

        Use read_many() to get an iterator over multiple rows.

        NOTE that we assume that a cursor exists; that this is a
        select statement; and that the statement itself is relatively
        sane and sanitized.
        """
        if self.offline:
            if self._debug:
                str_params = [ unicode(param) for param in params ]
                print "*** OFFLINE: not executing read " + \
                    "statement:\n      " + \
                    "%s\n    with" % ( statement ) + \
                    "\n        %s" % ( ", ".join( str_params ) )
            return None

        # Too hard and not needed yet...
        if results != 1:
            raise "Sorry, we can't read more than one row right now."

        # Stringify for debugging.
        if self._debug:
            str_params = [ unicode(param) for param in params ]
            print "*** Executing statement:\n      " + \
                "%s\n    with" % ( statement ) + \
                "\n        %s" % ( ", ".join( str_params ) )

        # Run the actual query!
        rows = self._cursor.execute( statement, params )
        result_row = self._cursor.fetchone()

        if self._debug:
            print "*** RESULT:"
            print result_row

        return result_row

    def read_many( self, statement, params ):
        """
        Execute a read statement and iterate over the result rows.
        Fake it if offline.

        Use read() to directly get a single result row instead.

        NOTE that we assume that this is a select statement, and that
        the statement itself is sane and sanitized.  We create a
        cursor for the use of this query; if other queries will be
        made incrementally based on the results of this one, a
        separate DBConnection object should be used.  Because MySQLdb
        does not have true cursors, we fake it with paged queries.
        """
        self.start()

        # Clean up the statement for pagination.
        _statement = statement.strip()
        _statement = _statement.strip(';')
        _statement = _statement.replace( '%', '%%' )
        _statement += " LIMIT " + str(PAGESIZE) + " OFFSET %d;"

        if self.offline:
            if self._debug:
                str_params = [ unicode(param) for param in params ]
                print "*** OFFLINE: not executing read " + \
                    "statement:\n      " + \
                    "%s\n    with" % ( statement ) + \
                    "\n        %s\n" % ( ", ".join( str_params ) )
            self.finish()
            raise StopIteration

        # Get a bunch of pages.
        while True:
            # Stringify for debugging.
            if self._debug:
                str_params = [ unicode(param) for param in params ]
                print "*** Executing statement:\n      " + \
                    "%s\n    with" % ( _statement %
                                       ( self._offset ) ) + \
                    "\n        %s" % ( ", ".join( str_params ) )

            # Run the query.
            result_size = self._cursor.execute( _statement %
                                                    ( self._offset ),
                                                params )
            self._offset += PAGESIZE

            if self._debug:
                print "*** RESULT: %i rows returned" % ( result_size )

            # If there were no results found, we’re done.
            if result_size <= 0:
                self.finish()
                raise StopIteration

            # Otherwise, generate the results.
            while True:
                result = self._cursor.fetchone()
                # If this page is done, fall back to the outer loop to
                # fetch another page.
                if result is None:
                    break
                if self._debug:
                    print "*** Next result row:"
                    print result
                yield result

    def start( self ):
        """
        Start a set of read or write transactions with a new cursor,
        or fake it if offline.
        """
        if not self.offline:
            self._cursor = self._db.cursor()
            self._offset = 0

        return

    def write( self, statement, params ):
        """
        Write to the database, if online; fake it if not.

        NOTE that we assume that a cursor exists; that this is an
        insert or update statement; and that the statement itself is
        relatively sane and sanitized.

        Args:
            statement: The statement to execute
            params: The parameters for the statement

        Returns:
            int: the number of rows affected as a result of executing
                 the statement (returns 0 if running in debug/test mode)
        """
        if not self._db_write:
            if self._debug:
                str_params = [ unicode(param) for param in params ]
                print "*** OFFLINE: not executing write " + \
                    "statement:\n      " + \
                    "%s\n    with" % ( statement ) + \
                    "\n        %s" % ( ", ".join( str_params ) )
            return 0

        # Stringify for debugging.
        if self._debug:
            str_params = [ unicode(param) for param in params ]
            print "*** Executing statement:\n      " + \
                "%s\n    with" % ( statement ) + \
                "\n        %s" % ( ", ".join( str_params ) )

        # Run the actual query!
        rows = self._cursor.execute( statement, params )

        if self._debug:
            print "*** RESULT: %i rows affected" % ( rows )

        return rows

class ORIALookupError( Exception ):
    """
    Raised when attempting to find or create a database entry
    completely fails.
    """
    pass
