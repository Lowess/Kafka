#!/usr/bin/env groovy
/**
 * Kafka 0.8.1.1
 * --- Rebalance partitions across the kafka cluster
 * @author Florian Dambrine
 */


@Grapes([
    @GrabConfig(systemClassLoader = true)
])

package com.gumgum.kafka

import org.apache.commons.cli.*

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

import java.lang.System

import com.gumgum.util.Logger

LOGGER = new Logger(clazz:this.getClass())

//////////////////////////////////////////////////////////////////////
// Command line options
//////////////////////////////////////////////////////////////////////

def options = new Options()
options.addOption("zookeeper", true, "DNS to connect to zookeeper")
options.addOption("kafka", true, "DNS to connect to one kafka in the cluster")
options.addOption("ssh_user", true, "Remote user used for ssh, the default value is set'ubuntu'")
options.addOption("ssh_key", true, "Absolute path to the ssh key")

options.addOption("excludeBroker", true, "Exclude broker given in this comma separated list")
options.addOption("excludeTopic", true, "Exclude topics given in this comma separated list")

options.addOption("maxPartitions", true, "Specify the maximum number of partitions to move contained in one Json part file")

options.addOption("run", false, "Run the repartitionment on the Kafka cluster")

CommandLineParser parser = new PosixParser()
CommandLine cmd = parser.parse(options, args)

//Remote connection
SSH_KEY = "/home/florian/.ssh/id_rskeypair"
if (cmd.hasOption("ssh_key")) {
    SSH_KEY = cmd.getOptionValue("ssh_key")
} else {
	usage()
	System.exit(99)
}

SSH_USER = "ubuntu"
if (cmd.hasOption("ssh_user")) {
    SSH_USER = cmd.getOptionValue("ssh_user")
}

//DNS of remote servers
ZK_CNX = "localhost"
if (cmd.hasOption("zookeeper")) {
    ZK_CNX = cmd.getOptionValue("zookeeper")
} else {
	usage()
	System.exit(97)
}

KAFKA_CNX = "localhost"
if (cmd.hasOption("kafka")) {
    KAFKA_CNX = cmd.getOptionValue("kafka")
} else {
	usage()
	System.exit(96)
}

//Generate exlusions lists
EXCLUSION = [
	"topic": [],
	"broker": []
]
if (cmd.hasOption("excludeBroker")) {
	EXCLUSION["broker"] = cmd.getOptionValue("excludeBroker").split(",")
}

if (cmd.hasOption("excludeTopic")) {
	EXCLUSION["topic"] = cmd.getOptionValue("excludeTopic").split(",")
}

//Trigger the --execute on Kafka
RUN = false
if (cmd.hasOption("run")) {
    RUN = true
}

//Specify the number of splits
PARTITION_SPLITS = 50
if (cmd.hasOption("maxPartitions")) {
    PARTITION_SPLITS = Integer.parseInt(cmd.getOptionValue("maxPartitions"))
}


//////////////////////////////////////////////////////////////////////
// Initialisation
//////////////////////////////////////////////////////////////////////

ANSI_RESET = "\u001B[0m";
ANSI_BLACK = "\u001B[30m";
ANSI_RED = "\u001B[31m";
ANSI_GREEN = "\u001B[32m";
ANSI_YELLOW = "\u001B[33m";
ANSI_BLUE = "\u001B[34m";
ANSI_PURPLE = "\u001B[35m";
ANSI_CYAN = "\u001B[36m";
ANSI_WHITE = "\u001B[37m";

//Path to the zkCli.sh on the remote server
ZK_CLI_SH = "/opt/zookeeper/zookeeper-3.4.5/bin/zkCli.sh"
//Path to kafka-reassign-partitions.sh
KAFKA_REASSIGN_SH = "/opt/kafka/bin/kafka-reassign-partitions.sh"

//Remote actions
ZK_CMD_LIST_TOPICS = "ls /brokers/topics | tail -n 1 | tr -d '\n'"
ZK_CMD_LIST_BROKER_IDS = "ls /brokers/ids | tail -n 1 | tr -d '\n'"

KAFKA_COPY = "cp /tmp/topics-to-move.json /tmp/topics-to-move.json"
KAFKA_GENERATE = "--zookeeper ${ZK_CNX}:2181 --topics-to-move-json-file /tmp/topics-to-move.json --generate --broker-list "
KAFKA_EXECUTE = "--zookeeper ${ZK_CNX}:2181 --reassignment-json-file /tmp/expand-cluster-reassignment.json --execute --broker-list "
KAFKA_VERIFY = "--zookeeper ${ZK_CNX}:2181 --reassignment-json-file /tmp/expand-cluster-reassignment.json --verify --broker-list "

//JSON Files and Folders
JSON_ROOT = "/tmp/kafka-partition-rebalancer"
JSON_FOLDERS = [
	"starter": "${JSON_ROOT}/starter",
	"parts": "${JSON_ROOT}/parts",
	"done": "${JSON_ROOT}/done"
]
JSON_FILES = [
	"starter": "topics-to-move.json",
	"parts": "reassign-part-" //Suffixed by a number
]

//////////////////////////////////////////////////////////////////////
//MAIN
//////////////////////////////////////////////////////////////////////

LOGGER.info(">> Start analyze.")

LOGGER.logIfThrows {
	def brokerIdList = []
	
    //Get the list of broker ids
    LOGGER.info(ANSI_PURPLE + "Requesting Zookeeper to get the broker id list..." + ANSI_RESET)
    brokerIdList = runRemoteCommand(ZK_CNX, ZK_CLI_SH, ZK_CMD_LIST_BROKER_IDS).trim().tokenize(',[]')
    if (brokerIdList.contains("WatchedEventstate:SyncConnectedtype:Nonepath:null")) {
		LOGGER.info(ANSI_RED + "Could not aquire the broker id list, try to re run the script" + ANSI_RESET)
    	System.exit(3)	
	}
	//Exclude
	brokerIdList.removeAll(EXCLUSION["broker"])
	

    LOGGER.info(ANSI_GREEN + "Broker id list = ${brokerIdList}" + ANSI_RESET)

	if (RUN) {
		reassignPartitions(brokerIdList)
	} else {
		prepare(brokerIdList)
	}
}

def reassignPartitions(def brokerIdList) {
	//Copy the reassignment Json file on the remote server
	partsFolder = new File(JSON_FOLDERS["parts"])
	partsFolder.eachFile { part -> 
		LOGGER.info(ANSI_PURPLE + "Uploading ${part.absolutePath} to the Kafka server (/tmp/expand-cluster-reassignment.json)..." + ANSI_RESET)

		def code = remoteCopyCommand(KAFKA_CNX, part.absolutePath , "/tmp/expand-cluster-reassignment.json")
		if (code == 0) {
			LOGGER.info(ANSI_GREEN + "Copy completed!" + ANSI_RESET)
		} else {
			LOGGER.info(ANSI_RED + "Remote copy failed! Script aborted" + ANSI_RESET)
			System.exit(5)
		}

		//Prepare the verify command
		def kafkaVerify = KAFKA_VERIFY + "\"" + brokerIdList.join(',') + "\" | tail -n +2 | grep -v \"ERROR\" | cut -d' ' -f4-"
		
		//Prepare the execute command
		def kafkaExecute = KAFKA_EXECUTE + "\"" + brokerIdList.join(',') + "\""

		LOGGER.info(ANSI_PURPLE + "Execute the kafka script to run the partition reassignment..." + ANSI_RESET)
		runRemoteCommand(KAFKA_CNX, KAFKA_REASSIGN_SH, kafkaExecute)

		def completed = false

		while (completed == false) {
			LOGGER.info(ANSI_PURPLE + "Running the kafka script to verify the partition reassignment..." + ANSI_RESET)
		
			//Verify the progress on the kafka server
			def verifyResponse = runRemoteCommand(KAFKA_CNX, KAFKA_REASSIGN_SH, kafkaVerify)
			
			//Projects the result in a map instead of using the result string
			def mapVerify = [:]
			verifyResponse.eachLine() { line ->
				split = line.split()
				mapVerify.put(split.first(), split.tail().join(" "))
			}

			completed = true
			mapVerify.each { k, v ->
				if (v.contains("completed successfully")) {
					LOGGER.info(ANSI_GREEN + "${k}\t\t${v}" + ANSI_RESET)	
				} else if (v.contains("is still in progress")) {
					completed = false
					LOGGER.info(ANSI_YELLOW + "${k}\t\t${v}" + ANSI_RESET)
				} else {
					completed = false
					LOGGER.info(ANSI_RED + "${k}\t\t${v}" + ANSI_RESET)
				}
			}
		}

		part.renameTo(new File(JSON_FOLDERS["done"], part.getName()))
	}
}

/**
 * Prepare the part files to reassign partitions accross the Kafka
 * cluster. This action has to be run before running the script
 * with --run option
**/
def prepare(def brokerIdList) {
	def topicList = []
	
	//Clearing files
	initFolders()

	//Get the list of topics
	LOGGER.info(ANSI_PURPLE + "Requesting Zookeeper to get the topic list..." + ANSI_RESET)
    topicList = runRemoteCommand(ZK_CNX, ZK_CLI_SH, ZK_CMD_LIST_TOPICS).trim().tokenize(',[]')
    if (topicList.contains("WatchedEventstate:SyncConnectedtype:Nonepath:null")) {
		LOGGER.info(ANSI_RED + "Could not aquire the topic list, try to re run the script" + ANSI_RESET)
    	System.exit(2)
    }
    //Exclusion
    topicList.removeAll(EXCLUSION["topic"])
	
    LOGGER.info(ANSI_GREEN + "Topic list = ${topicList}" + ANSI_RESET)
    
	//Generate the Json to rebalance all the topics topics-to-move.json
	LOGGER.info(ANSI_PURPLE + "Generating Json file..." + ANSI_RESET)
	generateJson(JSON_FOLDERS["starter"] + "/" + JSON_FILES["starter"], topicList)

	//Copy the previous json on the remote Kafka
	LOGGER.info(ANSI_PURPLE + "Copy the Json file on the kafka node..." + ANSI_RESET)
	def code = remoteCopyCommand(KAFKA_CNX, JSON_FOLDERS["starter"] + "/" + JSON_FILES["starter"], "/tmp/" + JSON_FILES["starter"])
	if (code == 0) {
		LOGGER.info(ANSI_GREEN + "Copy completed!" + ANSI_RESET)
	} else {
		LOGGER.info(ANSI_RED + "Remote copy failed! Script aborted" + ANSI_RESET)
		System.exit(4)
	}

	//Run the kafka tool
	LOGGER.info(ANSI_PURPLE + "Run the kafka script to generate the repartition plan..." + ANSI_RESET)
	def kafkaGenerate = KAFKA_GENERATE + "\"" + brokerIdList.join(',') + "\" | tail -n 1 | tr -d '\n'"
	
	//Get the Json returned by the Kafka server
	try {
	 	def slurper = new JsonSlurper()
		def jsonNewReplicaAssignment = slurper.parseText(runRemoteCommand(KAFKA_CNX, KAFKA_REASSIGN_SH, kafkaGenerate))
	 	
	    LOGGER.info(ANSI_GREEN + "Spliting repartition plan into Json part files (maximum ${PARTITION_SPLITS} partition(s) per file)" + ANSI_RESET)
		sliceJson(jsonNewReplicaAssignment, PARTITION_SPLITS)

		LOGGER.info(ANSI_GREEN + "The script has successfully preprared the reassignment plan. Now, run the script with the --run option to start moving partitions" + ANSI_RESET)
	} catch (Exception e) {
		LOGGER.info(ANSI_RED + "Failed to get the Json from the Kafka server, try to re run the script" + ANSI_RESET)
	}
}

def initFolders() {
	//Clear and create
	LOGGER.info(ANSI_PURPLE + "Clearing old files..." + ANSI_RESET)
	
	new File(JSON_ROOT).deleteDir()

   	JSON_FOLDERS.each { k,v -> 
   		new File(v).mkdirs()
	}
	
	LOGGER.info(ANSI_GREEN + "Done!" + ANSI_RESET)
}
def usage() {
    LOGGER.error("Usage: com.gumgum.kafka.KafkaPartitionRebalancer.groovy --ssh_key PATH_TO_SSH_KEY [--ssh_user REMOTE_USER=ubuntu] --zookeeper ZOOKEEPER_DNS --kafka KAFKA_DNS [--run]")
}


def getZookeeper() {

}

def runRemoteCommand(def host, def command, def args) {
	def process = "ssh -i ${SSH_KEY} ${SSH_USER}@${host} ${command} ${args}".execute()
	process.text
}

def remoteCopyCommand(def host, def src, def dst) {
	def process = "scp -i ${SSH_KEY} ${src} ${SSH_USER}@${host}:${dst}".execute()
	process.waitFor()
	process.exitValue()
}

def generateJson(def output, def topicList) {
	def topics = []

	topicList.each { topic ->
		def newTopic = ["topic": "${topic}"]
		topics << newTopic
	}				

	def data = [
  		"version": 1,
  		"topics": topics
	]

	def jsonFinal = new groovy.json.JsonBuilder(data)

	def topicToMove = new File(output)
	
	topicToMove.withWriter { out ->
		out.writeLine(jsonFinal.toString())
	}

	LOGGER.info(ANSI_GREEN + jsonFinal.toString() + ANSI_RESET)
}

def sliceJson(def jsonInput, def numPartition) {
	
	partitions = []
	split = 0
	part = 0
	jsonInput.partitions.each { partition ->
		def newPartition = [
			"topic": "${partition.topic}",
			"partition": partition.partition,
			"replicas": partition.replicas
		]
		
		partitions << newPartition

		split++

		if ((split == numPartition)) {
			def data = [
  				"version": 1,
  				"partitions": partitions
			]

			def jsonFinal = new groovy.json.JsonBuilder(data)
		
			def partitionsToMove = new File(JSON_FOLDERS["parts"] + "/" + JSON_FILES["parts"] + part + ".json")
			
			partitionsToMove.withWriter { out ->
				out.writeLine(jsonFinal.toString())
			}
			
			LOGGER.info("File ${JSON_FILES["parts"]}${part}.json: " + ANSI_PURPLE + jsonFinal.toString() + ANSI_RESET)

			partitions.clear()
			split = 0
			part++
		}
	}
	//Purge reminents
	if (split != 0) {
		def data = [
			"version": 1,
			"partitions": partitions
		]

		def jsonFinal = new groovy.json.JsonBuilder(data)

		def partitionsToMove = new File(JSON_FOLDERS["parts"] + "/" + JSON_FILES["parts"] + part + ".json")
			
		partitionsToMove.withWriter { out ->
			out.writeLine(jsonFinal.toString())
		}
			
		LOGGER.info("File ${JSON_FILES["parts"]}${part}.json: " + ANSI_PURPLE + jsonFinal.toString() + ANSI_RESET)

		part++
	}
}