hadoop dfs -mkdir /usr/greg/input
hadoop dfs -put input.txt /usr/greg/input/input.txt
mkdir cvs_classes
javac -cp /usr/lib/hadoop/hadoop-core.jar -d cvs_classes CVSPairThreshold.java 
jar -cvf cvs.jar -C cvs_classes/ .
hadoop jar cvs.jar CVSPairThreshold /usr/greg/input /usr/greg/output
hadoop dfs -cat /usr/greg/output/part-00000 
