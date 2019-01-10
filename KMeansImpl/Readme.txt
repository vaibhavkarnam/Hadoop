Implementing K-Means algorithm in MapReduce

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

Run Preprocessing.java first on the twitter dataset to create the followers data by
changing the job.name in make file to Preprocessing. 

Run MaxFollowers.java to find the max follower count for the dataset.This is given by the value after 
comma in the output. Divide this into k 
different centroids and manually create the centroids file with these newly found centers. Please provide
k as the argument while running this program.

run KMeans_Impl setting the local.file parameter in the header to the centroids file and also pass this
as argument in the makefile ${local.file}

run Postprocessing.java on the preprocessed input to generate the co-ordinates for x and y axis for the
plot

please be careful about the number of arguments you pass to the programs. preprocessing and postprocessing
need two arguments. the other two programs need 3 arguments  

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
