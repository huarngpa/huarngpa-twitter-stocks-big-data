#!/bin/bash

##########################################################################
# DELETE HUARNGPA HBASE TABLES
##########################################################################

echo "disable_all 'huarngpa.*'" | hbase shell
echo "drop_all 'huarngpa.*'" | hbase shell
