
# basic notes

flink on emr runs via yarn. the cluster by default does not have pyflink installed, so we need to get pyflink to every node in the cluster (its not enough to install it just on the master node).

The best way to get pyflink to all the nodes is by submitting a virtual environment through the cli command. 

Create the venv, install apache-flink. 

Now, when we submit a pyflink job via the command line, we can also submit the python venv so that it gets distributed to all the nodes. 

Since we need to specify a kafka connector, we need to also distribute the jar file for it between the clusters. 

If we uplaod that jar file to lib/ it should be accessible by all clusters.
we could also put our venv there i think, then it would be accessible to all clusters (instead of submitting through the CLI).

I think to access the flink web UI i need to use a SOCKS proxy. This really only works natively on Firefox. 





