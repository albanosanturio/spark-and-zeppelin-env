# spark-and-zeppelin-env

This compose file creates 1 master node, 2 worker nodes and a zeppelin standalone  

## Cleaning docker containers and images

If needed, the commands to get a clean docker would be:
```bash
docker stop $(docker ps -aq)
docker container rm -f $(docker container ls -aq)
docker image rm -f $(docker image ls -aq)
```

## Running compose file in terminal:  
  
```bash
docker-compose up -d
docker exec -it spark-master bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/testPy.py
```  
Go to localhost:8080 and see the results  
You should see the job done by the spark node under section "Applications"

## Loading local files

Use script in spark/apps/spark/loadLocalFiles.scala  
Example run:
```scala
val path = "/opt/spark-apps/avangrid_datastore.db/"
CreateExternalTable.loadDataYM(path)
```

NOTE: This scripts needs libraries com.typesafe.config and com.microsoft.azure.synapse  

```bash
spark-shell --jars /opt/spark-libraries/microsoft/azure/synapse/synapseutils_2.12/1.4/synapseutils_2.12-1.4.jar,/opt/spark-libraries/typesafe/config/1.4.1/config-1.4.1.jar -I /opt/spark-apps/loadLocalFiles.scala
```
