# Lab3-CSC364

Dataset: NYC Yellow Taxi Trip Data

Link: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

To start this lab:
1) Run: **stop-all.sh** inside of home terminal
2) Run: **start-all.sh**
3) Download the dataset above and place it into your Desktop, and rename it (ex/ dataset.csv)
4) Run: **cd Desktop**
5) Run: **hadoop fs -mkdir /lab3_input**
6) Run: **hadoop fs -put dataset.csv /lab3_input**
7) Double check that the dataset is in Hadoop (i.e http://localhost:9870 -> Utilities -> Browse the file system)

To run this lab:
0) Make sure to follow all the steps above first
1) Make a local copy of this repo (if there are any issues, click on the Maven tab on the right hand & click on the refresh button - in IntelliJ)
1) Assuming you are using IntelliJ, click on the Maven tab on the right hand
2) Click on the _Execute Maven Goal_ icon 
3) Click **clean** and enter
4) Click **install** and enter
5) Run: **hadoop jar target/Lab3-1.0-SNAPSHOT.jar org.example.Lab3 /lab3_input/dataset.csv /lab3_output** inside the project terminal (i.e. inside of IntelliJ). Make sure the pathways match your local project, mines might be different.
6) Wait until the map and reduce jobs are done, then check the output in Hadoop (i.e. check part_00000)
7) If you ever need to restart, you can delete the output file (**hdfs dfs -rm -r /lab3_output**), maven clean, maven install, then run #5 again 
