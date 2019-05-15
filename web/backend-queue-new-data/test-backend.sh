#!/bin/bash

##########################################################################
# VARS
##########################################################################

CURRENT_DIR=${PWD%}
WEB_ROOT=$CURRENT_DIR

##########################################################################
# SCRIPT
##########################################################################

echo "Testing Web Backend for Big Data System"
echo "$CURRENT_DIR"
echo "$WEB_ROOT"

## Remove SQLite database if it was last used (or in use)
if [ -f "${WEB_ROOT}/twitterstocksdb" ]; then
    rm "${WEB_ROOT}/twitterstocksdb"
    echo "Removed SQLite data. Starting over with a clean slate..."
fi

## Make the test database again
cd $WEB_ROOT
python3 "${WEB_ROOT}/manage.py" db upgrade
echo "Initialized the web database for testing."

## Start the server, blocks current bash session
python3 "${WEB_ROOT}/manage.py" runserver
