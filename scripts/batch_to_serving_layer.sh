#!/bin/bash

##########################################################################
# MANUAL WORKAROUND BATCH TO SERVING
##########################################################################

beeline << EOF

!connect jdbc:hive2://class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2

echo -en "\n\n"
!run batch_to_serving_layer.hql

EOF
