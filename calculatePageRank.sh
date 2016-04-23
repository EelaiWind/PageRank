# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

OUTPUT=pageRank/output/
BETA=0.85
TOTAL_NODE_COUNT=10877

for (( i = 0 ; i < $1; i=i+1 ))
do
#hadoop jar PageRank.jar PageRank.PageRankJob $OUTPUT 0.8 10877 
hadoop jar PageRank.jar PageRank.PageRankJob $OUTPUT $BETA $TOTAL_NODE_COUNT 
done

hadoop fs -cat ${OUTPUT}* > pageRank.log
