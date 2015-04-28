#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Generate STAR METRICS reports for a given fiscal quarter.

Written for the University of Illinois.
"""

__author__ = u"Christopher R. Maden <crism@illinois.edu>"
__date__ = u"4 April 2015"
__version__ = 1.6

# Adjust the load path for common data loading operations.
import sys
from os import path

CWD = path.dirname( path.abspath( __file__ ) )
LIB_PATH = path.join( path.dirname( CWD ), 'lib' )

import argparse
import csv
from datetime import date, timedelta
from getpass import getpass
import logging
from os import devnull, makedirs, remove
import re
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from time import localtime

# SQL query and connection constants
COAS = 1 # chart of accounts — Urbana campus only
SERVER = "reportprod.admin.uillinois.edu"
SERVICE = "REPTPROD"
SQLPLUS = LIB_PATH + "/sqlplus.sh"

# Data validation constants.
FIRST_FY = 1867 # oldest fiscal year allowed
FY_ROLLOVER = 7 # fiscal year increments in July
US_STATES = [ "AA", # Armed Forces, Americas
              "AE", # Armed Forces, Europe
              "AK",
              "AL",
              "AP", # Armed Forces, Pacific
              "AR",
              "AS", # American Samoa
              "AZ",
              "CA",
              "CO",
              "CT",
              "DC",
              "DE",
              "FL",
              "FM", # Micronesia
              "GA",
              "GU", # Guam
              "HI",
              "IA",
              "ID",
              "IL",
              "IN",
              "KS",
              "KY",
              "LA",
              "MA",
              "MD",
              "ME",
              "MH", # Marshall Is.
              "MI",
              "MN",
              "MO",
              "MP", # Northern Marianas
              "MS",
              "MT",
              "NC",
              "ND",
              "NE",
              "NH",
              "NJ",
              "NM",
              "NV",
              "NY",
              "OH",
              "OK",
              "OR",
              "PA",
              "PR",
              "PW", # Palau
              "RI",
              "SC",
              "SD",
              "TN",
              "TX",
              "UT",
              "VA",
              "VI", # Virgin Is.
              "VT",
              "WA",
              "WI",
              "WV",
              "WY" ]

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
                  "VendorPaymentAmount" ],
            "vendor_detail" :
                [ "PeriodStartDate",
                  "PeriodEndDate",
                  "VendorPIDM",
                  "UniqueAwardNumber",
                  "CFDACode",
                  "SponsorID",
                  "FundTypeCode",
                  "GrantTitle",
                  "RecipientAccountNumber",
                  "VendorAddress1",
                  "VendorAddress2",
                  "VendorCity",
                  "VendorState",
                  "VendorNation",
                  "VendorZIP",
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

class OracleDump( csv.excel ):
    """
    A CSV reader dialect, based on the Excel dialect, but with an
    idiosyncratic quote character.  Thank you, Oracle, for not
    providing any useful output tools!
    """
    quotechar = '}'

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

def construct_duns( state, nation, postcode ):
    """
    Create a pseudo-DUNS number out of a state, nation, and postal
    code.  Handle some weird edge cases.
    """
    state = state.strip() # These are probably not necessary...
    postcode = postcode.strip() # ... but the data is wonky.

    # Ideally, Z+postcode for US, F+postcode for foreign, but the
    # source data is bad.  The nation should have been corrected
    # before this is called.

    # We could add more structural validity checks here, but this is
    # the logic in the predecessor query.
    if nation == "US":
        if postcode == "":
            return "Z00000-0000"

        return "Z" + postcode

    if len( postcode ) > 9:
        return "F000000000"

    return "F" + postcode

def construct_unique_award_number( cfda, sponsor, ftype, title ):
    """
    Construct a unique award number from the CFDA code, sponsor ID,
    fund type, and grant title.
    """
    # Logic is from M.N.’s original SQL scripts, not fully understood
    # by this author.

    if cfda[3:7] == "000" or cfda[0:2] == "99":
        return "00.070 Federal - Other"

    if cfda.strip() == "":
        if ftype == "4A" or ftype == "4Y":
            cfda = "00.070"
        elif ftype == "4G":
            cfda = "00.200"
        else:
            cfda = "00.000"
    elif cfda == "93.848":
        cfda = "93.847"

    if sponsor.strip() == "":
        sponsor = title

    return cfda + " " + sponsor

def correct_nation( state, nation ):
    """
    Given a state or province and a nation, makes US the explicit
    nation if the state supports that conclusion.
    """
    state = state.strip() # These are probably not necessary...
    nation = nation.strip() # ... but the data is wonky.

    # Normally, a null nation means USA, but there are many Canadian
    # provinces given without nation, and null state codes; assume
    # they’re all foreign.
    if nation == "" and state in US_STATES:
        nation = "US"

    return nation

def handle_vendor_line( sql_line, csv_writer, csv_writer_detail ):
    """
    Given a line of quoted, comma-delimited SQL output and a CSV
    writer linked to a destination file, interpret the fields as a
    STAR METRICS vendor report, and write to the output file.
    """
    # Making a new reader instance for every line is inefficient, but
    # given the highly heterogeneous input, I’m not sure how else to
    # do this.
    csv_reader = csv.reader( sql_line.splitlines(1),
                             dialect=OracleDump )

    # For each row, generate the proper STAR METRICS report, but also
    # output the fully-detailed version.
    for row in csv_reader:
        # The first two fields are dates; use as-is.  The third is the
        # PIDM, for detail.
        sm_fields = row[0:2]
        det_fields = row[0:3]

        # Create the UniqueAwardNumber out of the CFDA code, sponsor
        # ID, fund type, and grant title.
        unique_award = construct_unique_award_number( row[3],
                                                      row[4],
                                                      row[5],
                                                      row[6] )
        sm_fields.append( unique_award )
        det_fields.append( unique_award )

        # Add the RecipientAccountNumber
        sm_fields.append( row[7] )

        # Build the pseudo-DUNS number out of the state, nation, and
        # ZIP or postal codes.
        corrected_nation = correct_nation( row[11], row[12] )
        pseudo_duns = construct_duns( row[11],
                                      corrected_nation,
                                      row[13] )
        sm_fields.append( pseudo_duns )

        # Add the award source information, account number, address,
        # and pseudo-DUNS to the detail report.
        det_fields += row[3:12]
        det_fields.append( corrected_nation )
        det_fields.append( row[13] )
        det_fields.append( pseudo_duns )

        # Add the VendorPaymentAmount.
        sm_fields.append( row[14] )
        det_fields.append( row[14] )

        # Write out the STAR METRICS report and the detailed report.
        csv_writer_detail.writerow( det_fields )
        csv_writer.writerow( sm_fields )

    return

def write_csv_line( sql_line, csv_writer, report_type ):
    """
    Given a line of SQL fixed-width output, CSV writer linked to a
    destination file, and a report type, interpret the fields in the
    line according to the report type, and write the CSV equivalent to
    the output file.
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
    elif report_type == "subaward":
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
                         default="24999",
                         help="dollar floor for vendor/subaward " +
                             "transaction inclusion" )
    parser.add_argument( "--outdir", "-o", type=str, default=".",
                         help="directory in which to place output " +
                             "files" )
    parser.add_argument( "--logfile", "-l", default=devnull,
                         type=argparse.FileType('w'),
                         help="log file (default none)" )
    parser.add_argument( "--debug", "-d", action="store_true",
                         help="don’t delete temp SQL file" )
    args = parser.parse_args()

    # Start logging.
    logfile = args.logfile
    logging.basicConfig(
        stream=logfile,
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logging.info( "Beginning STAR METRICS report." )

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
    end_date_str = str( end_date ).replace( "-", "_" ) # for filename

    # Normalize strings for SQL consumption.
    beg_date_sql = start_date.strftime( "%d-%b-%Y" ).upper()
    end_date_sql = end_date.strftime( "%d-%b-%Y" ).upper()
    fy_sql = "%02d" % ( fy % 100 )
    periods = [ "%02d" % (((quarter-1)*3) + i + 1 ) for i in range(3) ]

    # Generate a PL/SQL file with the given parameters.
    logging.info( "Generating SQL master file..." )
    logging.info( "  beg_date: %s" % beg_date_sql )
    logging.info( "  coas: %d" % COAS )
    logging.info( "  end_date: %s" % end_date_sql )
    logging.info( "  fsyr: %s" % fy_sql )
    logging.info( "  lowerlimit: %d" % args.vendor_floor )
    logging.info( "  periods: %s" % ", ".join( periods ) )

    wfile = NamedTemporaryFile( dir=".", delete=False, suffix=".sql" )
    wfile.write( "DEFINE beg_date = '%s';\n" % beg_date_sql )
    wfile.write( "DEFINE coas = '%d';\n" % COAS )
    wfile.write( "DEFINE end_date = '%s';\n" % end_date_sql )
    wfile.write( "DEFINE fsyr = '%s';\n" % fy_sql )
    wfile.write( "DEFINE lowerlimit = %d;\n" % args.vendor_floor )
    for i in range(3):
        wfile.write( "DEFINE period%d = '%s';\n" %
                     ( i+1, periods[i] ) )
    wfile.write( """SET FEEDBACK OFF;
SET HEADING OFF;
SET LINESIZE 1000;
SET PAGESIZE 0;
SET TRIMSPOOL ON;
""" )
    wfile.write( "@" + CWD + "/star_metrics_award.sql\n" )
    wfile.write( "@" + CWD + "/star_metrics_subaward.sql\n" )
    wfile.write( "@" + CWD + "/new_vendor.sql\n" )
    wfile.write( "@" + CWD + "/star_metrics_employee.sql\n" )
    wfile.close()

    # Run sqlplus on the file.  Capture the output.
    rfile = open( wfile.name, "r" )
    sqlplus = Popen( "%s %s/%s@%s/%s" %
                     ( SQLPLUS, args.user, pwd, SERVER, SERVICE ),
                     shell=True, stdin=rfile, stdout=PIPE )

    # Set some initial state variables.
    errors = False
    in_vars = False
    outfile = None
    outfile_detail = None
    report_idx = 0
    writer = None
    writer_detail = None
    var_sub_re = re.compile( VAR_SUB_RE )

    # Read the results.
    for line in sqlplus.stdout:
        # Next, parse the output and drop it in the -o specified
        # directory in four pieces, in CSV format, with appropriate
        # headers.

        # Look for errors above all else.  The error message comes out
        # on the line after ERROR:, so report that line.
        if errors:
            logging.error( line )
            raise StarMetricsAuthenticationError, "\n" + line

        if line.startswith( "ERROR:" ):
            logging.error( line )
            errors = True
            continue

        # If we are writing to a file and get a SQL prompt, the output
        # is done.
        if outfile:
            if line.startswith( "SQL>" ):
                writer = None
                logging.info( "Closing output file(s)." )
                outfile.close()
                outfile = None
                if outfile_detail:
                    writer_detail = None
                    outfile_detail.close()
                    outfile_detail = None
                report_idx += 1
            elif report == "vendor":
                handle_vendor_line( line, writer, writer_detail )
            else:
                write_csv_line( line, writer, report )
            continue

        # When we are between files, the old/new lines will tell us
        # that variable substitution has happened and results are
        # about to begin.
        if outfile is None and not in_vars and \
                var_sub_re.match( line ):
            logging.info( "Variable substitution for next output " + \
                          "file has begun." )
            in_vars = True
            continue

        # If we were in variable substitution and it ends, then
        # results have started.
        if in_vars and not( var_sub_re.match( line ) ):
            in_vars = False
            report = REPORTS[ report_idx ]
            outfn = "%s_%s_%s.csv" % ( UNIV_ABBR,
                                       report.capitalize(),
                                       end_date_str )
            logging.info( "Opening output file %s" % outfn )
            outfile = open( path.join( outdir, outfn ), "w" )
            writer = csv.writer( outfile )
            writer.writerow( HEADERS[ report ] )
            if report == "vendor":
                outfn_detail = "%s_%s_detail_%s.csv" % \
                               ( UNIV_ABBR,
                                 report.capitalize(),
                                 end_date_str )
                logging.info( "Opening output file %s" %
                              outfn_detail )
                outfile_detail = open( path.join( outdir,
                                                  outfn_detail ),
                                       "w" )
                writer_detail = csv.writer( outfile_detail )
                writer_detail.writerow(
                    HEADERS[ report + "_detail" ] )
                handle_vendor_line( line, writer, writer_detail )
            else:
                write_csv_line( line, writer, report )
            continue

    # Remove the generated file.
    rfile.close()
    if not args.debug:
        remove( wfile.name )

    logging.info( "Finishing STAR METRICS reports." )

    return

if __name__ == '__main__':
    main()
    exit( 0 )
