#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Protect faculty IDs in appointment data.

Given a CSV where the first two columns are the EDW ID and the PIDM,
drop the EDW ID and encrypt the PIDM.  While we’re at it, nuke fields
whose value is all whitespace.

The CSV file must have a header whose first two labels are EDW_PERS_ID
and BANNER_PIDM.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"25 November 2014"
__version__ = 1.0

KEY_MAX = 0xffffff

import argparse
import csv
import sys

# Encrypt the faculty IDs; get the encryption key silently from STDIN.
from getpass import getpass

class FacultyInfoError( Exception ):
    """
    Raised if there is something wrong with the input file.
    """
    pass

def main():
    """
    Read in a command-line specified CSV and emit the modified output.
    """
    # Parse user options.
    parser = argparse.ArgumentParser(
        description=u"protect faculty IDs in appointment data"
    )
    parser.add_argument( "--out", "-o", type=argparse.FileType('w'),
                         default=sys.stdout,
                         help=u"output file (STDOUT by default)" )
    parser.add_argument( "file", type=open, help=u"CSV file to read" )
    args = parser.parse_args()

    #Get the encryption key.
    id_key = 0
    key_str = getpass( u"What number between 1 and " +
                       u"%d " % KEY_MAX +
                       u"shall we use for encryption? " )
    while id_key < 1 or id_key > KEY_MAX:
        try:
            id_key = int(key_str)
        except:
            key_str = getpass( u"Please try again, using only " +
                               u"digits.  Enter a number between " +
                               u"1 and %d. " % KEY_MAX )

        if id_key < 1 or id_key > KEY_MAX:
            key_str = getpass( u"Please try again.  " +
                               u"The key must be a number between " +
                               u"1 and %d. " % KEY_MAX )

    # Start the reading and writing.
    reader = csv.reader( args.file )
    writer = csv.writer( args.out )

    # Check the input header
    header = reader.next()
    if header[0] != "EDW_PERS_ID" or \
            header[1] != "BANNER_PIDM":
        raise FacultyInfoError, \
            u"Input header must start with EDW_PERS_ID and " + \
            u"BANNER_PIDM."

    # Alter the header appropriately and write it.
    header = [ "FACULTY_ID" ] + header[2:]
    writer.writerow( header )

    # Handle each row.
    for row in reader:
        # Clean up blank values.
        for idx in range( len( row) ):
            row[idx] = row[idx].strip()

        edw_id = int(row[0])

        # Write the altered row with the encrypted ID.
        writer.writerow( [ edw_id ^ id_key ] + row[2:] )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
