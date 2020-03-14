#!/bin/bash

epoch=1

while true
do
    echo "$epoch"
    for sim_time in 1000 1500 2000 3000 5000 10000 20000
    do
        for mean_msg_arrivalint in 0.01 0.1 0.2 0.3 0.5 0.8 1
        do
            for mean_msg_size in 50 80 100 150 200 300
            do
                for outoforder in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
                do
                    for loss in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
                    do
                        for corrupt in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
                        do
                            echo "Epoch $epoch: $sim_time $mean_msg_arrivalint $mean_msg_size $outoforder $loss $corrupt"
                            ./rdt_sim $sim_time $mean_msg_arrivalint $mean_msg_size $outoforder $loss $corrupt 0 > rdt_sim.log
                            if [ $(grep -c "\<Congratulations\>" rdt_sim.log) -lt 1 ]; 
                            then 
                                echo "ERROR: something wrong !"
                                exit
                            fi
                        done
                    done
                    clear
                done
            done
        done
    done
    let 'epoch++'
    clear
done



