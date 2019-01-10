
We study the small world phenomenon in the Twitter follower graph. Intuitively, the small-world phenomenon states that any two people are connected through a short sequence of acquaintances. More formally, the shortest path between any two users should consist of only a few edges, say 5 to 10. To verify this claim, we want to compute the diameter of the Twitter graph. It is defined as the “longest shortest path between any pair of users.” Exact computation is expensive, and several approximation techniques exist. We will work with a simple approximation algorithm as follows:
1. Given a parameter k, randomly sample approximately k distinct users from the Twitter data.
2. Run k-source shortest path for all k users together. This computes the shortest path from each of the k source vertices to all vertices in the graph (including the sources). It is similar to the single-source program but requires maintaining the distances from each of the k source vertices, instead of just a single source. (This not the same as running single-source shortest path k times!)
3. Return the longest shortest path for each of the k source vertices.

Installation.

Please make sure the following compnents are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment Setup:

example bash alias configurations
Please add these configs to your ~/.bashrc file

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/usr/local/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution of the program:

Run Preprocessing.java first on the twitter dataset to create the adjacenecyList and inital distances by
changing the job.name in make file to Preprocessing. 

Give the output as input to TwitterShortestPath and also change the job.name back

All of the build & execution commands are organized in the Makefile.
1 Open command prompt.
2 Navigate to directory where project files unzipped.
3 Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.

example makefile environment customization (customie this for your environment):

hadoop.root=/usr/local/hadoop
jar.name=twitter-foll-mr-1.0.jar
jar.path=target/${jar.name}
job.name=wc.TwitterFollAggregator
local.input=input
local.output=output
# Pseudo-Cluster Execution
hdfs.user.name=vaibhav
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=vaibhav-mrdemo
aws.subnet.id=subnet-395fd217
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large


For Standalone Hadoop:
	make switch-standalone	 //set standalone Hadoop environment (execute once)
	make local

For AWS EMR Hadoop: (you must configure all the emr.* config parameters at top of Makefile)
	make upload-input-aws    //run only before first execution
	make aws		 //Executes on AWS.check for successful execution with web interface after completetion (aws.amazon.com)
	download-output-aws      //after successful execution & termination to download the output files
