#! /usr/bin/env bash
# RACE=-race
echo "Running test $1 for $2 trials" >> testout.txt

for i in $(seq 1 $2); do
    echo "$i / $2" >> testout.txt
    LOG="$1_$i.txt"
    timeout -k 2s 200s go test $RACE -run $1 > $LOG 
    if [[ $? -eq 0 ]]; then
	rm $LOG
    else
	echo "Failed at trial $i, saving log at $LOG" >> testout.txt
    fi
done
