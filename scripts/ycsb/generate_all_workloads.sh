#!/bin/bash


KEY_TYPE=randint
# KEY_TYPE=monoint
for WORKLOAD_TYPE in a b c; do
  echo workload${WORKLOAD_TYPE} > workload_config.inp
  echo ${KEY_TYPE} >> workload_config.inp
  python2 gen_workload.py workload_config.inp
  mv workloads/load_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/load${WORKLOAD_TYPE}_unif_int.dat
  mv workloads/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE} workloads/txns${WORKLOAD_TYPE}_unif_int.dat
done

