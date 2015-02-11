export LD_LIBRARY_PATH=/content/discovery/lib/instantclient10_1:$LD_LIBRARY_PATH
export ORACLE_HOME=/content/discovery/lib/instantclient10_1
$ORACLE_HOME/sqlplus $@
