#!/bin/bash

nc -lk 9998 | 
 while read msg; do
 	ts=$(date +%s%3N); 
	read v1 v2 v3 v4 <<< $msg; 
	echo "$msg $ts" $(($ts - $v2)) $(($ts - $v3)) >> latency.txt; 
 done