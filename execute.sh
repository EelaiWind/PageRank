# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/WordCount/Output/
#hadoop jar WordCount.jar wordcount.WordCount /user/shared/WordCount/Input /user/TA/WordCount/Output
#hdfs dfs -cat /user/TA/WordCount/Output/part-*

HOME=/user/s101062105/pageRank
INPUT=${HOME}/input/
TMP_OUTPUT=${HOME}/tmp_output/

#hadoop jar PageRank.jar PageRank.AdjacencyListJob $INPUT $OUTPUT 10877
hadoop jar PageRank.jar PageRank.AdjacencyListJob $INPUT 10877
hadoop fs -cat ${TMP_OUTPUT}/* > adjacencyList.log

