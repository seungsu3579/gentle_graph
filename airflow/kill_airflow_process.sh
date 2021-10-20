#!/bin/bash

echo 'kill airflow process'
kill -9 `ps -ef | grep airflow | grep -v grep | awk '{print $2}'`
sleep 1

echo 'kill chrome process'
kill -9 `ps -ef | grep chromedriver | grep -v grep | awk '{print $2}'`
sleep 1
kill -9 `ps -ef | grep chrome | grep -v grep | awk '{print $2}'`

/opt/google/chrome/chrome