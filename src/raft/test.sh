#!/bin/bash

rm -rf res
mkdir res

for ((i = 0; i < 10; i++))
do

    for ((c = $((i*30)); c < $(( (i+1)*30)); c++))
    do                  #replace job name here
         (go test -run 2B ) &> ./res/$c & 
    done
    
    sleep 40
    
    grep -nr "FAIL.*raft.*" res

done