#########################################################################
# File Name: merge.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: 三  8/26 16:35:45 2015
#########################################################################
# !/bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd ${PATH_NOW}

# source something

cd ${PATH_PRE}

PROJECT_DIR="/home/houjp/NewsRecommendation/"
LOG_DIR=${PROJECT_DIR}/log/
DATA_DIR="hdfs://10.100.1.50:8020/user/lihb/"

HOST="10.100.1.50"
PORT="8124"

function shut() {
	curl -X POST http://${HOST}:${PORT}/stop
}

function merge() {
	date=`date -d last-day +%Y-%m-%d`
	pre_date=`date -d "2 days ago" +%Y-%m-%d`

	echo "[INFO] date = "${date}
	echo "[INFO] pre_date = "${pre_date}

	has_docs=true
	hdfs dfs -ls ${DATA_DIR}/docs/${date}.txt
	if [ 0 -ne $? ]; then
		has_docs=false
	else
		size=`hdfs dfs -count ${DATA_DIR}/docs/${date}.txt | awk '{print $3}'`
		if [ 0 -eq ${size} ]; then
			has_docs=false
		fi 
	fi
	
	has_user=true
	hdfs dfs -ls ${DATA_DIR}/user_info/${date}.txt
	if [ 0 -ne $? ]; then
		has_user=false
	fi

	echo "[INFO] has_docs = "${has_docs}
	echo "[INFO] has_user = "${has_user}

	echo "[INFO] boot offline ..."
	${PROJECT_DIR}/bin/boot.sh offline ${pre_date} ${date} ${DATA_DIR} ${has_docs} ${has_user} &> ${LOG_DIR}/offline.${date}.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] boot offline failed."
		return 1
	else
		echo "[INFO] boot offline success."
	fi

	echo "[INFO] boot online ..."
	${PROJECT_DIR}/bin/boot.sh online ${date} ${DATA_DIR} ${HOST} ${PORT} &> ${LOG_DIR}/online.${date}.log
	if [ 0 -ne $? ]; then
		echo "[ERROR] boot online failed."
	else
		echo "[INFO] boot online success."
	fi
}

shut
merge

# 0	3	*	*	*	${PROJECT_DIR}/bin/merge.sh &>> ${LOG_DIR}/merge.log &
