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
source ../conf/conf.sh
cd ${PATH_PRE}

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
