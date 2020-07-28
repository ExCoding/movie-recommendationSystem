# 启动 flume

./bin/flume-ng agent -c ./conf/ -f ./conf/log-kafka.properties -n a1 -Dflume.root.logger=INFO,console
