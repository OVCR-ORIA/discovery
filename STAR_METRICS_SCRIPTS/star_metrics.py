#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Generate STAR METRICS reports for a given fiscal quarter.

Must be run in the same directory as the STAR METRICS SQL scripts.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"24 February 2015"
__version__ = 1.4

# Adjust the load path for common data loading operations.
import sys
from os import path
sys.path.append(
    path.join(
        path.dirname( path.dirname( path.abspath( __file__ ) ) ),
        'lib'
        )
    )

# Common data loading tools.
import oria

import argparse
import csv
from datetime import date, timedelta
from getpass import getpass
from os import makedirs, remove
import re
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from time import localtime

# SQL query and connection constants
COAS = 1 # chart of accounts — Urbana campus only
SERVER = "reportprod.admin.uillinois.edu"
SERVICE = "REPTPROD"
#SQLPLUS = path.join( path.dirname( path.abspath( __file__ ) ),
#                     "sqlplus.sh" ) # SQL*Plus command
SQLPLUS = "/content/discovery/lib/sqlplus.sh"

# Date validation constants.
FIRST_FY = 1867 # oldest fiscal year allowed
FY_ROLLOVER = 7 # fiscal year increments in July

# CSV output constants.
HEADERS = { "award" : # CSV header lines
                [ "PeriodStartDate",
                  "PeriodEndDate",
                  "UniqueAwardNumber",
                  "RecipientAccountNumber",
                  "OverheadCharged" ],
            "employee" :
                [ "PeriodStartDate",
                  "PeriodEndDate",
                  "UniqueAwardNumber",
                  "RecipientAccountNumber",
                  "DeidentifiedEmployeeIdNumber",
                  "OccupationalClassification",
                  "FteStatus",
                  "ProportionOfEarningsAllocatedToAward" ],
            "subaward" :
                [ "PeriodStartDate",
                  "PeriodEndDate",
                  "UniqueAwardNumber",
                  "RecipientAccountNumber",
                  "SubAwardRecipientDunsNumber",
                  "SubAwardPaymentAmount" ],
            "vendor" :
                [ "PeriodStartDate",
                  "PeriodEndDate",
                  "UniqueAwardNumber",
                  "RecipientAccountNumber",
                  "VendorDunsNumber",
                  "VendorPaymentAmount" ] }
REPORTS = [ "award", "subaward", "vendor", "employee" ]
UNIV_ABBR = "UIUC" # University name for filenames

# Parsing constants.
AWARD_REMNANT_RE = r'(.*\S)\s+(\S+)\s+([-.0-9]+)$'
EMPLOYEE_REMNANT_RE = r'(.*\S)\s+(\S+)\s+([0-9A-F]+)\s+(\S.*\S)' + \
    r'\s+([.0-9]+)\s+([.0-9]+)$'
SQL_LINE_START_RE = r'^(^[0-9]{4}-[0-9]{2}-[0-9]{2}) ' + \
    r'([0-9]{4}-[0-9]{2}-[0-9]{2}) ' + \
    r'([0-9]{2}\.[0-9]{3}) (.*)$'
SUB_VENDOR_FALLBACK_RE = r'(.*\S)\s+(\S+)\s+([-.0-9]+)$'
SUB_VENDOR_REMNANT_RE = r'(.*\S)\s+(\S+)\s+([ZF].{0,10})\s+([-.0-9]+)$'
VAR_SUB_RE = r'^[no][el][dw]\s*[0-9]+:' # Variable subs in SQL output

# Not really constants but hey.
award_remnant_re = re.compile( AWARD_REMNANT_RE )
employee_remnant_re = re.compile( EMPLOYEE_REMNANT_RE )
sql_line_start_re = re.compile( SQL_LINE_START_RE )
sub_vendor_fallback_re = re.compile( SUB_VENDOR_FALLBACK_RE )
sub_vendor_remnant_re = re.compile( SUB_VENDOR_REMNANT_RE )

class StarMetricsAuthenticationError( Exception ):
    """
    Raised when the SQL*Plus client fails to connect for some reason
    (most likely authentication).
    """
    pass

class StarMetricsDateError( Exception ):
    """
    Raised when a requested date is out of range.
    """
    pass

class StarMetricsOutputError( Exception ):
    """
    Raised when SQL*Plus output does not match the expected format.
    """
    pass

def write_csv_line( sql_line, csv_writer, report_type ):
    """
    Given a line of SQL fixed-width output, a destination file, and a
    report type, interpret the fields in the line according to the
    report type, and write the CSV equivalent to the output file.
    """
    # Everything starts with start date, end date, unique award, and
    # recipient.
    m = sql_line_start_re.match( sql_line.strip() )
    if m is None:
        raise StarMetricsOutputError, \
            "SQL output line does not match expectations:\n" + \
            sql_line

    fields = [ m.group(1),  # start date
               m.group(2) ]  # end date
    cfda_no = m.group(3)

    if report_type == "award":
        award_m = award_remnant_re.match( m.group(4) )
        if award_m is None:
            raise StarMetricsOutputError, \
                "SQL output line does not match expectations:\n" + \
                sql_line
        fields.extend( [ "%s %s" %  # award ID
                             ( cfda_no, award_m.group(1) ),
                         award_m.group(2), # acct. no.
                         float( award_m.group(3) ) ] ) # amount
    elif report_type == "subaward" or report_type == "vendor":
        fallback = False
        sub_vendor_m = sub_vendor_remnant_re.match( m.group(4) )
        if sub_vendor_m is None:
            fallback = True
            sub_vendor_m = sub_vendor_fallback_re.match( m.group(4) )
            if sub_vendor_m is None:
                raise StarMetricsOutputError, \
                    "SQL output line does not match " + \
                    "expectations:\n" + sql_line
        fields.extend( [ "%s %s" %  # award ID
                             ( cfda_no, sub_vendor_m.group(1) ),
                         sub_vendor_m.group(2) ] ) # acct. no.
        if fallback:
            fields.extend( [
                    "", # DUNS/ZIP
                    float( sub_vendor_m.group(3) ) # amount
                    ] )
        else:
            fields.extend( [
                    sub_vendor_m.group(3).strip(), # DUNS/ZIP
                    float( sub_vendor_m.group(4) ) # amount
                    ] )
    elif report_type == "employee":
        employee_m = employee_remnant_re.match( m.group(4) )
        if employee_m is None:
            raise StarMetricsOutputError, \
                "SQL output line does not match expectations:\n" + \
                sql_line
        fields.extend( [
                "%s %s" % ( cfda_no, employee_m.group(1) ), # award ID
                employee_m.group(2), # acct. no.
                employee_m.group(3), # emp. ID
                employee_m.group(4), # emp. class
                float( employee_m.group(5) ), # FTE
                float( employee_m.group(6) ) # prop. alloc.
                ] )

    csv_writer.writerow( fields )
    return

def main():
    """
    Given a quarter, generate a PL/SQL script, then execute it,
    catching the output.
    """
    # Parse user options.
    parser = argparse.ArgumentParser(
        description="generates STAR METRICS reports for a quarter"
    )
    parser.add_argument( "--user", "-u", type=str, required=True,
                         help="SQL*Plus username for DB access" )
    parser.add_argument( "--fy", type=int, required=True,
                         help="fiscal year for which to generate " +
                             "results" )
    parser.add_argument( "--quarter", "-q", type=int,
                         choices=[1,2,3,4], required=True,
                         help="quarter for which to generate " +
                             "results" )
    parser.add_argument( "--vendor-floor", "--vf", type=int,
                         default="25000",
                         help="dollar floor for vendor " +
                             "transaction inclusion" )
    parser.add_argument( "--outdir", "-o", type=str, default=".",
                         help="directory in which to place output " +
                             "files" )
    args = parser.parse_args()

    # Make sure the output directory exists and is writable.
    outdir = path.abspath( args.outdir )
    if not( path.exists( outdir ) ):
        makedirs( outdir )

    pwd = getpass( "What’s the magic word? " )

    # Determine the current fiscal year.
    now = localtime()
    if now.tm_mon >= FY_ROLLOVER:
        this_fy = now.tm_year + 1
    else:
        this_fy = now.tm_year

    # To find the current quarter:
    # 1) Subtract one from the month to enable modulo arithmetic.
    # 2) Add the rollover offset — the rollover month minus one.
    # 3) Take that mod 12 to get the month, or period, of the fiscal
    #    year.
    # 4) Divide by 3 in the integer realm to get the zero-indexed
    #    quarter (0–3).
    # 5) Add 1 to get the 1-based number of the quarter (1–4).
    # Easy!
    this_q = (((now.tm_mon - 1 + (FY_ROLLOVER - 1)) % 12) / 3) + 1

    # Normalize the fiscal year.
    if args.fy >= 0 and args.fy < 100:
        if args.fy >= 70:
            fy = 1900 + args.fy
        else:
            fy = 2000 + args.fy
    else:
        fy = args.fy
    quarter = args.quarter

    if fy < FIRST_FY:
        raise StarMetricsDateError, \
            "Requested fiscal year is too early."
    elif fy > this_fy:
        raise StarMetricsDateError, \
            "Requested fiscal year is in the future."
    elif fy == this_fy:
        if quarter == this_q:
            raise StarMetricsDateError, \
                "Requested quarter is not yet complete."
        elif quarter > this_q:
            raise StarMetricsDateError, \
                "Requsted quarter is still in the future."

    # Determine the dates covered by the requested quarter.
    start_mon = (((FY_ROLLOVER-1) + ((quarter-1)*3)) % 12) + 1
    if start_mon >= FY_ROLLOVER:
        start_cal_year = fy-1
    else:
        start_cal_year = fy
    start_date = date( start_cal_year, start_mon, 1 )

    end_mon = ((start_mon + 2) % 12) + 1
    if end_mon < start_mon:
        end_cal_year = start_cal_year + 1
    else:
        end_cal_year = start_cal_year
    end_date = date( end_cal_year, end_mon, 1 ) - timedelta( 1 )
    end_date_str = str( end_date ).replace( "-", "_" )

    # Generate a PL/SQL file with the given parameters.
    wfile = NamedTemporaryFile( dir=".", delete=False, suffix=".sql" )
    wfile.write( "DEFINE beg_date = '%s';\n" %
                 start_date.strftime( "%d-%b-%Y" ).upper() )
    wfile.write( "DEFINE coas = '%d';\n" % COAS )
    wfile.write( "DEFINE end_date = '%s';\n" %
                 end_date.strftime( "%d-%b-%Y" ).upper() )
    wfile.write( "DEFINE fsyr = '%02d';\n" % ( fy % 100 ) )
    wfile.write( "DEFINE lowerlimit = %d;\n" % args.vendor_floor )
    for i in range(3):
        wfile.write( "DEFINE period%d = '%02d';\n" %
                     ( i+1, ((quarter-1)*3) + i + 1 ) )
    wfile.write( """SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET PAGESIZE 0;
SET TRIMSPOOL ON;
@star_metrics_award.sql
@star_metrics_subaward.sql
@star_metrics_vendor.sql
@star_metrics_employee_anon.sql
""" )
    wfile.close()

    # Run sqlplus on the file.  Capture the output.
    # @@@ TODO: can we get the SQL*Plus process to read from STDIN?
    rfile = open( wfile.name, "r" )
    sqlplus = Popen( "%s %s/%s@%s/%s" %
                     ( SQLPLUS, args.user, pwd, SERVER, SERVICE ),
                     shell=True, stdin=rfile, stdout=PIPE )

    # Set some initial state variables.
    errors = False
    in_vars = False
    outfile = None
    report_idx = 0
    writer = None
    var_sub_re = re.compile( VAR_SUB_RE )

    # Read the results.
    for line in sqlplus.stdout:
        # Next, parse the output and drop it in the -o specified
        # directory in four pieces, in CSV format, with appropriate
        # headers.

        # If we are writing to a file and get a SQL prompt, the output
        # is done.
        if outfile:
            if line.startswith( "SQL>" ):
                writer = None
                outfile.close()
                outfile = None
                report_idx += 1
            else:
                write_csv_line( line, writer, report )
            continue

        # When we are between files, the old/new lines will tell us
        # that variable substitution has happened and results are
        # about to begin.
        if outfile is None and not in_vars and \
                var_sub_re.match( line ):
            in_vars = True
            continue

        # If we were in variable substitution and it ends, then
        # results have started.
        if in_vars and not( var_sub_re.match( line ) ):
            in_vars = False
            report = REPORTS[ report_idx ]
            outfile = open( path.join( outdir,
                                       "%s_%s_%s.csv" %
                                           ( UNIV_ABBR,
                                             report.capitalize(),
                                             end_date_str ) ),
                            "w" )
            writer = csv.writer( outfile )
            writer.writerow( HEADERS[ report ] )
            write_csv_line( line, writer, report )
            continue

        # Otherwise, look for connection errors.  The error message
        # comes out on the line after ERROR:, so report that line.
        if errors:
            raise StarMetricsAuthenticationError, "\n" + line

        if line.startswith( "ERROR:" ):
            errors = True

    # Remove the generated file.
    rfile.close()
    remove( wfile.name )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
