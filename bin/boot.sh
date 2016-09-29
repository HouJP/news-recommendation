#########################################################################
# File Name: boot.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: 二  9/27 14:19:10 2016
#########################################################################
#! /bin/bash


# 加载配置文件
PATH_PRE=`pwd`
PATH_NOW=`dirname $0`
cd ${PATH_NOW}
source utils.sh
source ../conf/conf.sh
cd ${PATH_PRE}

# Stop the port if is running
function stop() {
	local flag=`lsof -i:${PORT} | wc -l`
	if [ 0 -ne $flag ]; then
		log "INFO" "port(${PORT}) is running ..."
		curl -X POST "http://${HOST}:${PORT}/golaxy/recommend/news/stop"
		if [ 0 -ne $? ]; then
			return 255
		fi
	else
		log "INFO" "port($PORT) isn't running."
	fi
	return 0
}

function run() {
	local class="com.bda.recommendation.news.service.Boot"

	${SPARK_HOME}/bin/spark-submit \
		--master ${MASTER} \
		--class ${class} \
		--conf spark.cores.max=${SPARK_CORES_MAX} \
		--conf spark.executor.memory=${SPARK_EXECUTOR_MEMORY} \
		--conf spark.driver.memory=${SPARK_DRIVER_MEMORY} \
		${JAR_PT} 

	return $?
}

function boot() {
	stop
	if [ 0 -ne $? ]; then
		log "INFO" "stop port(${PORT}) failed!"
	else
		log "INFO" "stop port(${PORT}) succeed."
	fi

	run
	if [ 0 -ne $? ]; then
		log "INFO" "run service failed!"
	else
		log "INFO" "run service succeed."
	fi

	return 0
}

boot