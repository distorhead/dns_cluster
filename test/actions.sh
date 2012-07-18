#!/bin/bash

sudo rm -rf /var/lib/bind/*
sudo python -i -c 'from test.actions import *'
