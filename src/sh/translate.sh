#!/usr/bin/env

HOST="202.102.83.169"
PASSWD="h0use@365.2016.combigData"
spawn scp -r /Users/wubingyu/IdeaProjects/365company_demo/demo/target/demo-1.0-SNAPSHOT -P 22151 root@$HOST:/home
expect "*password:" { send "$PASSWD\n" }
expect eof