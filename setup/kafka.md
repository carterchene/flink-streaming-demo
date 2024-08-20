Kafka and Eventsim live on an EC2 instance. Kafka itself is accessible on port 9092, and the Kafka web UI is available on 9021.

- ssh into the kafka-server vm.

- Download git and docker.

```bash
    sudo yum install git
    sudo yum install docker-compose
```

- Clone repo

```bash
    git clone https://github.com/carterchene/flink-streaming-demo.git
```

- Cd to kafka

```bash
    cd kafka
```

- Edit the docker-compose.yml 
```bash
    nano docker-compose.yml 
```
    I couldn't get the docker-compose file to take the kafka ip as an environment variable. So I just set it using nano.
    Change the KAFKA_ADVERTISED_LISTENERS section in the docker-compose to be the IP of the kafka EC2. Ctrl+X, Y, Enter. (save and exit)

- Start Kafka
```bash
    sudo docker-compose up
```

- View the web ui
If you configure the security group on the VM to allow your IP TCP access on port 9021, you can go to ip.of.kafka.server:9021 to view the Kafka web ui.

- Open a new terminal, ssh into the server again. 

- Cd to scripts

- Run the script
```bash
    sudo bash eventsim_setup.sh
```

This will setup eventsim and should start streaming data to kafka topics in a few minutes. You can view this in the kafka web ui. 

