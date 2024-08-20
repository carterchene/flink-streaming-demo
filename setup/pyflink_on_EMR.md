## Flink on EMR

- ssh into primary node

- start flink session
```bash
    flink-yarn-session -d
```

- now the Flink Web UI should be up. I couldn't get foxyproxy or chrome to work with a dynamic proxy to connect to the flink ui. Instead, I portforwarded. The output from the flink-yarn-session will tell you what port the flink ui is on. Port forward to that port using something like:

```bash
    ssh -i ssh-key.pem -N -L 8080:ip-12-3-45-678.ca-central-1.compute.internal:12345 hadoop@ec2-12-345-678-9101.ca-central-1.compute.amazonaws.com
```

- Need to download the required jar files to the cluster. Download them to the "lib" libary: 
```bash
    cd ../../lib
```

- Get the jar files from mvn repository. [Here](https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka) is the kafka one. 

- I uploaded them all to S3.

- Download them on the cluster with aws cli: 

```bash
    aws s3 cp s3://my-bucket/jars/ . --recursive
```

- The flink script will look for some env vars. Set them:
```bash 
    export KAFKA_BROKER_IP=your.kafka.server.ip
    export S3_BUCKET=my_bucket
```

- Now we can run the pyflink script. Clone the repo if you haven't already and submit the job via:
```bash 
    flink run -py stream_all_events.py
```

- View the flink UI. You should see 4 running jobs. This is also where you will troubleshoot from.

- Check that files are getting written to your S3 bucket. Should take about 5 minutes.
