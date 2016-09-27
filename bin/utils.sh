#########################################################################
# File Name: utils.sh
# Author: HouJP
# mail: houjp1992@gmail.com
# Created Time: 二  9/27 14:49:05 2016
#########################################################################
#! /bin/bash

function log() {
	local typ=$1
	local msg=$2

	echo "[`date`] [$typ] $msg"
}