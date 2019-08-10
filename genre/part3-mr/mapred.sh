python dir.py

hadoop jar /Users/kelly/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar \
-D mapred.map.tasks=10  \
-D mapred.reduce.tasks=10 \
-file /Users/kelly/Desktop/proj/draft/mapper.py \
-file /Users/kelly/Desktop/proj/draft/reducer.py \
-mapper /Users/kelly/Desktop/proj/draft/mapper.py \
-reducer /Users/kelly/Desktop/proj/draft/reducer.py \
-input hdfs://localhost:9000/test.txt \
-output /gmr
