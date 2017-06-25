#!/bin/bash
i=0;

while : ;
	do echo "$i" $(date +%s%3N);
	let "i++";
	sleep $1;
done |
nc -lk localhost 9999