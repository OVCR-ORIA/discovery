DEFINE beg_date = '01-OCT-2013';
DEFINE end_date = '31-DEC-2013';
DEFINE fsyr = '14';
DEFINE period1 = '04';
DEFINE period2 = '05';
DEFINE period3 = '06';
DEFINE coas = '1';
SET LINESIZE 1000;
SET PAGESIZE 0;
SET TRIMSPOOL ON;
SET FEEDBACK OFF;
SPOOL /content/discovery/STAR_METRICS_SCRIPTS/awards;
@star_metrics_award.sql
SPOOL OFF;
SPOOL /content/discovery/STAR_METRICS_SCRIPTS/subawards;
@star_metrics_subaward.sql
SPOOL OFF;
SPOOL /content/discovery/STAR_METRICS_SCRIPTS/vendors;
@star_metrics_vendor.sql
SPOOL OFF;
SPOOL /content/discovery/STAR_METRICS_SCRIPTS/employees;
@star_metrics_employee_anon.sql
SPOOL OFF;

