#!/bin/bash

source config.sh

sudo apt update
sudo apt-get install cmake
sudo apt-get install python2
sudo apt-get install python3
sudo apt-get install python3-pip
sudo apt-get install python3-venv
sudo apt-get install python3-dev
sudo apt-get install python3-setuptools
sudo apt-get install python3-wheel
sudo apt-get install python3-numpy
sudo apt-get install python3-scipy
sudo apt-get install python3-matplotlib
sudo apt-get install python3-pandas
sudo apt-get install -y openjdk-11-jdk
sudo apt install -y libssl-dev
sudo apt install -y numactl
sudo pip3 install psrecord
sudo pip3 install seaborn
sudo apt update && sudo apt install -y texlive-full

# YCSB
bash scripts/ycsb/ycsb.sh
