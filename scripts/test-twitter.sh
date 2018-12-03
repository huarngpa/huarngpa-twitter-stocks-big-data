#!/bin/bash

##########################################################################
# VARS
##########################################################################

CURRENT_DIR=${PWD%}
PROJECT_ROOT=${PWD%/*}
WEB_ROOT="${PROJECT_ROOT}/web/backend-queue-new-data"

##########################################################################
# SCRIPT
##########################################################################

echo "Testing Twitter Big Data System"
echo "$CURRENT_DIR"
echo "$PROJECT_ROOT"
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

## Go back to the original directory
cd $CURRENT_DIR
