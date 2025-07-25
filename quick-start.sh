
1. docker-compose up -d --build

# Wait for the container to be up and running
# then run the following commands

2. docker exec -u 0 -it itvdelab cp /usr/bin/python3 /usr/bin/python

3. docker exec -it itvdelab bash -c "/opt/spark3/sbin/start-history-server.sh"

# 4, and 5 are required to set the ownership of the directories if you faced problems with permissions
4. docker exec -u 0 -it itvdelab chown -R itversity:itversity /home/itversity/spark
5. docker exec -u 0 -it itvdelab chown -R itversity:itversity /home/itversity/hive


6. docker exec -it itvdelab bash -c "cp hive/hive-site.xml /opt/hive/conf/"
7. docker exec itvdelab nohup hive --service hiveserver2 &> services/hive/hiveserver2.log 2>&1 &
# OR run: docker exec -it itvdelab bash -c "nohup hive --service hiveserver2 > /home/itversity/hive/hiveserver2.log 2>&1 &"
# after running the above command, you can check the logs using:
# docker exec -it itvdelab tail -f /home/itversity/hive/hiveserver2.log
# wait for 3 minutes until Hive Sesson ID is generated
    # Hive Session ID = af205067-...........................
    # Hive Session ID = e9b28f30-..............-f39cc2af1968
    # Hive Session ID = 3cc.......................f51582daeb
    # Hive Session ID = e4a767b6-...........................

8. docker exec -it itvdelab bash /home/itversity/spark/bronze/load_bronze.sh
# this command will load the data from shared-server to bronze layer
# under /home/itversity/bronze on hdfs


10. docker exec -it itvdelab bash -c "/opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/ports_to_silver.py"
# you can go to hive and check the data in silver database
#SELECT * FROM silver.ports;

11. docker exec -it itvdelab bash -c "/opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/vessels_to_silver.py"
# you can go to hive and check the data in silver database
# SELECT * FROM silver.vessels WHERE destination_port_lon IS NOT NULL;


12. docker exec -it itvdelab bash -c "/opt/spark3/bin/spark-submit --master local[2] --conf spark.ui.port=18181 /home/itversity/spark/silver/seismic_to_silver.py"
# you can go to hive and check the data in silver database
# SELECT * FROM silver.seismic;


# Date is Loaded into Silver Layer
# Now we can load the data from silver to gold layer
# open jupyter and open spark/gold/questions.ipynb 
# and run the cells to load the data into gold layer


# Feel free to test or add new queries in the questions.ipynb file
# to answer the questions related to the data in gold layer


