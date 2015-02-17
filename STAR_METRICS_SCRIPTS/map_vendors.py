#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
prepare STAR METRICS vendor info for mapping

Reads a STAR METRICS vendor report; alters pseudo-DUNS numbers, which
are actually postal codes, to put back their postal code status.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"13 February 2015"
__version__ = 1.0

import argparse
import csv
import re
import sys

DUNS_COL = 4
FOREIGN_RE = r'^F(.*)$'
VENDOR_HEADER = [ "PeriodStartDate", "PeriodEndDate",
                  "UniqueAwardNumber", "RecipientAccountNumber",
                  "VendorDunsNumber", "VendorPaymentAmount" ]
ZIP_RE = r'^Z([0-9]{5})([^0-9]?[0-9]+)?$'

class VendorInfoError( Exception ):
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
        description=u"prepare STAR METRICS data for mapping"
    )
    parser.add_argument( "--out", "-o", type=argparse.FileType('w'),
                         default=sys.stdout,
                         help=u"output file (STDOUT by default)" )
    parser.add_argument( "file", type=open, help=u"CSV file to read" )
    args = parser.parse_args()

    # Compile our REs.
    foreign_re = re.compile( FOREIGN_RE )
    zip_re = re.compile( ZIP_RE )

    # Start the reading and writing.
    reader = csv.reader( args.file )
    writer = csv.writer( args.out )

    # Check the input header
    header = reader.next()
    if header != VENDOR_HEADER:
        raise VendorInfoError, \
            u"Input header must match STAR METRICS requirements."

    # Alter the header appropriately and write it.
    header += [ "USA", "PostalCode" ]
    writer.writerow( header )

    # Handle each row.
    for row in reader:
        # Check the pseudo-DUNS number.
        pduns = row[DUNS_COL]

        # Look for a Z and US ZIP code:
        m = zip_re.match( pduns )
        if m is not None:
            row += [ "Y", m.group(1) ]
        else:
            # Look for an F and a foreign postal code.
            m = foreign_re.match( pduns )
            if m is not None:
                row += [ "N", m.group(1) ]
            else:
                row += [ "U", None ]

        # Write the altered row ...
        writer.writerow( row )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
