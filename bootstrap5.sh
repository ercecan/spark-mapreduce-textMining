#!/bin/bash
sudo yum update -y && \
sudo python3 -m pip install urllib3==1.26.6 && \
sudo python3 -m pip install spacy &&  \
sudo python3 -m spacy download en_core_web_sm

# 5th iteration. This worked...