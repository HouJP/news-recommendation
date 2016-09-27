#########################################################################
# File Name: boot.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: Wed Mar 16 15:34:38 2016
#########################################################################
#! /bin/bash

# 加载配置文件
PATH_PRE=`pwd`
PATH_NOW=`dirname $0`
cd ${PATH_PRE}
../conf/conf.sh
cd ${PATH_NOW}

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

function runJob() {
	local date=$1
	local class=$2

	submit $date $class
	if [ 0 -ne $? ]; then
		echo "[ERROR] run class($class), date($date) meet error!"
		return 255
	else
		echo "[INFO] run class($class), date($date) success."
	fi
}

function run() {
	local date=$1
	local class=

	# offline processor of key word recommendation
	class="com.bda.recommendation.keywords.KeyWordsOfflineProcessor"
	runJob $date $class

	# offline processor of events recommendation
	class="com.bda.recommendation.events.EventsOfflineProcessor"
	runJob $date $class

	# boot service
	class="com.bda.recommendation.service.Boot"
	runJob $date $class
}

if [ 1 -ne $# ]; then
	echo "Usage: run.sh <date>"
	exit 1
fi

date=$1
run $date
