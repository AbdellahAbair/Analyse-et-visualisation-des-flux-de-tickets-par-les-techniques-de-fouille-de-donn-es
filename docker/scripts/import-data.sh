#!/bin/bash

apt-get update && apt-get install -y wget
wget -O /tmp/elis.json "http://media.mongodb.org/elis.json"
mongoimport --db=elis --collection=zips /tmp/elis.json