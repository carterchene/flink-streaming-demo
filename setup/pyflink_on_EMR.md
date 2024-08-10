
# basic notes

flink on emr runs via yarn. the cluster by default does not have pyflink installed, so we need to get pyflink to every node in the cluster (its not enough to install it just on the master node).

The best way to get pyflink to all the nodes is by submitting a virtual environment through the cli command. 

Create the venv, install apache-flink. 

Now, when we submit a pyflink job via the command line, we can also submit the python venv so that it gets distributed to all the nodes. 

Since we need to specify a kafka connector, we need to also distribute the jar file for it between the clusters. 

If we uplaod that jar file to lib/ it should be accessible by all clusters.
we could also put our venv there i think, then it would be accessible to all clusters (instead of submitting through the CLI).

I think to access the flink web UI i need to use a SOCKS proxy. This really only works natively on Firefox. 

I think the steps are as follows:

create the yarn session with
flink-yarn-session

then you have to submit jobs to that specific yarn session created. so get the application id via
yarn application -list

then do something like: flink run -m yarn-cluster -yid appication_idfromabove -yn 2 job.jar 


##########################
# execute the statement set
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    statement_set.execute().wait()

look here for how to write proper script:
https://nightlies.apache.org/flink/flink-docs-master/api/python/examples/datastream/connectors.html

for s3 sink, use s3a:// so it knows its using hadoop and not presto


Alright i was able to access the flink ui through port forwarding. I ran a flink job via
flink run -py word_count.py

that gave this:
Found Web Interface ip-10-0-20-117.ca-central-1.compute.internal:43217

I accessed it by port forwarding like this: 
ssh -i kafka-cluster.pem -N -L 8080:ip-10-0-20-117.ca-central-1.compute.internal:43217 hadoop@ec2-15-222-245-242.ca-central-1.compute.amazonaws.com

# Set up instructions

1. Fire up the EMR cluster and the kafka EC2. (this will be terraform later)
2. ssh into kafka ec2. cd to kafka, change the kafka listener ip in docker-compose.yml to appropriate. docker-compose up
3. cd to scripts, run bash eventsim_setup.sh. Now events are streaming to kafka topics. verify at ip.of.kafka.server:9021 (this is a webui that lets us see whats goin on)
4. ssh to primary node of flink cluster. download our script: aws s3 cp s3://s3bucketname/scripts/stream_all_events.py .
4. create flink session: flink-yarn-session -d
5. note where the jobmanager ui is hosted (it says in the logs of flink-yarn-session). portfoward to see the flink ui (subbing in for appropriate addresses): ssh -i kafka-cluster.pem -N -L 8080:ip-10-0-20-117.ca-central-1.compute.internal:43217 hadoop@ec2-15-222-245-242.ca-central-1.compute.amazonaws.com
6. set env variables for the pyflink job: export KAFKA_BROKER_IP=ip.addy and export S3_BUCKET=mys3 
7. once that works, lets run the flink job. run flink run -py stream_all_events.py from primary node
8. in flink ui, should see 4 jobs begin. parquets should be getting made in s3 bucket. 
