#########################################################################
# File Name: boot.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Wed Mar 16 15:34:38 2016
#########################################################################
#! /bin/bash

PATH_PRE=`pwd`
PATH_NOW=`dirname $0`
cd ${PATH_PRE}
# source something
cd ${PATH_NOW}

MASTER=spark://golaxy50:7077
JAR_PT=/home/houjp/recommendation/jar/recommendation.jar
SPARK_HOME=/opt/spark/
SPARK_CORES_MAX=3
SPARK_EXECUTOR_MEMORY=5g
SPARK_DRIVER_MEMORY=5g

function submit() {
	local date=$1
	local class=$2

	${SPARK_HOME}/bin/spark-submit \
		--master ${MASTER} \
		--class ${class} \
		--conf spark.cores.max=${SPARK_CORES_MAX} \
		--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
		--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
		${JAR_PT} \
		--date ${date}

	return $?
}

function run() {
	local date=$1
	local class=

	# offline processor of key word recommendation
	class="com.bda.recommendation.keywords.KeyWordsOfflineProcessor"
	submit $date $class
	if [ 0 -ne $? ]; then
		echo "[ERROR] run $class meet error!"
		return 255
	else
		echo "[INFO] run $class success."
	fi

	# offline processor of events recommendation
	class="com.bda.recommendation.events.EventsOfflineProcessor"
	submit $date $class
	if [ 0 -ne $? ]; then
		echo "[ERROR] run $class meet error!"
		return 255
	else
		echo "[INFO] run $class success."
	fi

	# boot service
	class="com.bda.recommendation.service.Boot"
	submit $date $class
	if [ 0 -ne $? ]; then
		echo "[ERROR] run $class meet error"
		return 255
	else
		echo "[INFO] run $class success."
	fi
}

if [ 1 -ne $# ]; then
	echo "[ERROR] ./run.sh <date>"
	exit 1
fi

date=$1
run $date
