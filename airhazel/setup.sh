#!/bin/bash
mvn clean package
cd client/target/
tar xvzf airhazel-client-1.0-SNAPSHOT-bin.tar.gz
cd ../../
cd server/target/
tar xvzf airhazel-server-1.0-SNAPSHOT-bin.tar.gz
cd airhazel-server-1.0-SNAPSHOT
chmod +x run-server.sh
./run-server.sh
