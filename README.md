# spark-and-zeppelin-env

## Let's create an environment with a spark node and a zeppelin notebook

This compose file creates 1 master node, 2 worker nodes and a zeppelin standalone  

## Cleaning docker containers and images

If needed, the commands to get a clean docker would be:
```bash
docker stop $(docker ps -aq)
docker container rm -f $(docker container ls -aq)
docker image rm -f $(docker image ls -aq)
```

## in terminal

```bash
docker-compose up -d
docker exec -it spark-master bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/test.py
```
go to localhost:8080 and see the results  
You should see the job done by the spark node under section "Applications"
