# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

OUTPUT=pageRank/output/
BETA=0.85
TOTAL_NODE_COUNT=10877
RANK_SUM=${TOTAL_NODE_COUNT}

hadoop jar PageRank.jar PageRank.PageRankJob $OUTPUT $BETA $TOTAL_NODE_COUNT $RANK_SUM $1

hadoop fs -cat ${OUTPUT}* > pageRank.log
