#########################################################################
# File Name: conf.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: 六  9/17 15:22:29 2016
#########################################################################
#! /bin/bash

# Project 配置
PROJECT_DIR="/home/recommendation/"
LOG_DIR=${PROJECT_DIR}/log/
# Jar 配置
JAR_PT=${PROJECT_DIR}/target/scala-2.10/NewsRecommendation-assembly-1.0.jar

# Service 配置
HOST="10.100.1.50"
PORT="8488"

# Spark 配置
MASTER=spark://10.1.111.15:7077
SPARK_HOME=/opt/spark/
SPARK_CORES_MAX=1
SPARK_EXECUTOR_MEMORY=5g
SPARK_DRIVER_MEMORY=5g

