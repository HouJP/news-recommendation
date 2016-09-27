#########################################################################
# File Name: boot.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: 二  8/25 17:35:00 2015
#########################################################################
# !/bin/bash

PATH_PRE="`pwd`"
PATH_NOW="`dirname $0`"
cd ${PATH_NOW}

# source something

cd ${PATH_PRE}

CLASS=com.bda.model.Boot
MASTER=spark://golaxy50:7077
JAR_PT=/home/houjp/NewsRecommendation/jar/NewsRecommendation-assembly-1.0.jar
SPARK_HOME=/opt/spark/
SPARK_CORES_MAX=5
SPARK_EXECUTOR_MEMORY=5g
SPARK_DRIVER_MEMORY=5g

function init() {
	date=$1
	data=$2

	${SPARK_HOME}/bin/spark-submit \
		--master ${MASTER} \
		--class ${CLASS} \
		--conf spark.cores.max=${SPARK_CORES_MAX} \
		--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
		--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
		${JAR_PT} \
		--data ${data} \
		--boot "init" \
		--pre_time ${date} 

	return $?
}

function offline() {
	pre_date=$1
	date=$2
	data=$3
	has_docs=$4
	has_user=$5

	${SPARK_HOME}/bin/spark-submit \
		--master ${MASTER} \
		--class ${CLASS} \
		--conf spark.cores.max=${SPARK_CORES_MAX} \
		--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
		--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
		${JAR_PT} \
		--data ${data} \
		--boot "offline" \
		--pre_time ${pre_date} \
		--time ${date} \
		--has_docs ${has_docs} \
		--has_user ${has_user}

	return $?
}

function online() {
	date=$1
	data=$2
	host=$3
	port=$4
	
	${SPARK_HOME}/bin/spark-submit \
		--master ${MASTER} \
		--class ${CLASS} \
		--conf spark.cores.max=${SPARK_CORES_MAX} \
		--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
		--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
		${JAR_PT} \
		--data ${data} \
		--boot "online" \
		--time ${date} \
		--host ${host} \
		--port ${port}

	return $?
}

if [ 1 -gt $# ]; then
	echo "[ERROR] Usage: ./boot.sh <init|offline|online> [options...]"
	exit 1
fi

cmd=$1
if [ "init"x = ${cmd}x ]; then
	if [ 3 -ne $# ]; then
		echo "[ERROR] Usage: ./boot.sh <init> <date> <data_dir>"
		echo "[ERROR] Usage: eg. ./boot.sh init 2015-08-17 hdfs://bda00:8020/user"
	else
		echo "[INFO] boot init ..."
		init $2 $3
		if [ 0 -ne $? ]; then
			echo "[ERROR] boot init failed."
		else
			echo "[INFO] boot init success."
		fi
	fi
elif [ "offline"x = ${cmd}x ]; then
	if [ 6 -ne $# ]; then
		echo "[ERROR] Usage: ./boot.sh <offline> <pre_date> <date> <data_dir> <has_docs> <has_user>"
		echo "[ERROR] Usage: eg. ./boot.sh offline 2015-08-17 2015-08-18 hdfs://bda00:8020/user/houjp/NewsRecommendation/data true true"
	else
		echo "[INFO] boot offline ..."
		offline $2 $3 $4 $5 $6
		if [ 0 -ne $? ]; then
			echo "[ERROR] boot offline failed."
		else
			echo "[INFO] boot offline success."
		fi
	fi
elif [ "online"x = ${cmd}x ]; then
	if [ 5 -ne $# ]; then
		echo "[ERROR] Usage: ./boot.sh <online> <date> <data_dir> <host> <port>"
		echo "[ERROR] Usage: eg. ./boot.sh online 2015-08-18 hdfs://bda00:8020/user/houjp/NewsRecommendation/data 10.60.0.57 8124"
	else
		echo "[INFO] boot online ..."
		online $2 $3 $4 $5
		if [ 0 -ne $? ]; then
			echo "[ERROR] boot online failed."
		else
			echo "[INFO] boot online success."
		fi
	fi
else
	echo "[ERROR] Usage: ./boot.sh <init|offline|online> [options...]"
	exit 1
fi
