#########################################################################
# File Name: merge.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Wed Mar 16 17:18:21 2016
#########################################################################
#! /bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd ${PATH_NOW}
# source something
cd ${PATH_PRE}

PROJECT_DIR="/home/houjp/recommendation/"
LOG_DIR=${PROJECT_DIR}/log/

HOST="10.100.1.50"
PORT="8488"

function shut() {
	curl -X POST http://${HOST}:${PORT}/golaxy/recommend/stop
}

function merge() {
	date=`date -d last-day +%Y-%m-%d`

	echo "[INFO] date=$date"

	echo "[INFO] boot ..."
	${PROJECT_DIR}/bin/boot.sh $date
	if [ 0 -ne $? ]; then
		echo "[ERROR] boot failed."
		return 255
	else
		echo "[INFO] boot success."
	fi
}

shut
merge
