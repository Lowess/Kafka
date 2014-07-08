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
// Classes
//////////////////////////////////////////////////////////////////////

/**
 *
*/
class PartitionAnalyzer {
	private static final PartitionAnalyzer instance = new PartitionAnalyzer()
	private List<Broker> brokerList

	private PartitionAnalyzer() {
		//EC2 construct the bk list
		this.brokerList = [
			new Broker("101461782", "Kafka 1", "localhost"),
			new Broker("101461702", "Kafka 2", "localhost"),
			new Broker("101671664", "Kafka 3", "localhost"),
			new Broker("105114483", "Kafka 4", "localhost"),
			new Broker("101811991", "Kafka 5", "localhost"),
			new Broker("101421743", "Kafka 6", "localhost"),
			new Broker("101812541", "Kafka 7", "localhost"),
			new Broker("101862816", "Kafka 8", "localhost"),
			new Broker("102311671", "Kafka 9", "localhost")
		]
	}

	public static PartitionAnalyzer getInstance() {
		return instance
	}

	public List<Broker> getBrokerList() {
		return this.brokerList
	}

	public Integer getClusterOwnedPartitionNumber() {
		Integer clusterOwnedPartitionNumber = 0
		brokerList.each { broker ->
			clusterOwnedPartitionNumber += broker.getOwnedPartitionNumber()
		}
		clusterOwnedPartitionNumber
	}

	public Integer getClusterOwnedTopicNumber() {
		Integer clusterOwnedTopicNumber = 0
		brokerList.each { broker ->
			if (broker.getTopicCounter() > clusterOwnedTopicNumber){
				clusterOwnedTopicNumber = broker.getTopicCounter()
			}
		}
		clusterOwnedTopicNumber
	}

	public Broker searchBroker(String brokerId) {
		Broker searchBk = null
		this.brokerList.each { bk ->
			if (bk.brokerId == brokerId) {
				searchBk = bk
			}
		}
		searchBk
	}

	/**
	 * Finds the broker which is under the average number of partitions
	 * and has the lowest number of partition for a specific topic.
	**/
	public Broker findDestinationBroker(String topicName, Integer partitionAvg) {
		Broker brokerDest = null
		this.brokerList.each { broker ->
			Topic targetedTopic = broker.getTopic(topicName)
			//A broker is automatically selected if it does not own the topic
			if (targetedTopic == null) {
				brokerDest = broker
			} else if (targetedTopic.getPartitionCounter() < partitionAvg) {
				//If the broker as less partitions than the selected one
				if ( (brokerDest == null) || (targetedTopic.getPartitionCounter() < brokerDest.getTopic(topicName).getPartitionCounter()) ) {
					brokerDest = broker
				}
			}
		}
		brokerDest
	}

	/**
	 * Return the number of partitions for which the broker is the leader
	**/
	public Integer getPartitionLeaderCount(String brokerId, String topicName) {
		Integer leaderCount = 0
		//Count the number of partition in the topic topicName on which brokerId is leader
		this.brokerList.each { broker ->
			//Search the related topic
			Topic searchedTopic = broker.getTopicList().find { it.getTopicName() == topicName }
			if (searchedTopic != null) {
				searchedTopic.getPartitionList().each { partition ->
					//If the given broker is leader on this partition increment leaderCount
					if (partition.getLeader().getBrokerId() == brokerId) {
						leaderCount++
					}
				}
			}
		}
		leaderCount
	}
}

/**
 *
*/
class Broker {
	private String brokerId
	private String tagName
	private String publicIp
	private List<Topic> topicList
	
	public Broker (String brokerId, String tagName, String publicIp) {
		this.brokerId = brokerId
		this.tagName = tagName
		this.publicIp = publicIp
		this.topicList = []
	}

	public void addTopicWithPartitionsAndReplicas (String topicName, String partitionName, List replicaList) {
		if (!this.topicExists(topicName)) {
			this.topicList << new Topic(topicName, partitionName, replicaList)
			//Sort the list
			this.topicList = this.topicList.sort { it.getTopicName() }
		}
	}

	public Boolean topicExists (String topicName) {
		Boolean found = false
		this.topicList.each { t ->
			if (t.getTopicName() == topicName) {
				found = true
			}
		}
		found
	}

	public Topic getTopic (String topicName) {
		Topic topic = null
		this.topicList.each { t ->
			if (t.getTopicName() == topicName) {
				topic = t
			}
		}
		topic
	}

	public Integer getOwnedPartitionNumber() {
		Integer ownedPartitionNumber = 0
		topicList.each { topic ->
			ownedPartitionNumber += topic.getPartitionCounter()
		}
		ownedPartitionNumber
	}

	//////////////////////////////////////////////////////////////////
	//GETTERS
	
	public String getBrokerId() {
		this.brokerId
	}

	public String getTagName() {
		this.tagName
	}

	public String getPublicIp() {
		this.publicIp
	}

	public Integer getTopicCounter() {
		this.topicList.size()
	}

	public List<Topic> getTopicList() {
		this.topicList
	}
}


/**
 *
*/
class Topic {
	private String topicName
	private List<Partition> partitionList
	private List<String> partitionLock //Disable reassignment for partitions contained in this list

	public Topic(String topicName, String partition, List replicaList) {
		this.topicName = topicName
		this.partitionList = []
		this.partitionLock = []
		this.partitionList << new Partition(partition, replicaList)
	}

	public void addPartition(String partition, List replicaList) {
		this.partitionList << new Partition(partition, replicaList)
		this.partitionList = this.partitionList.sort { it.partitionNum }
	}

	public Partition getPartition(String partitionNum) {
		this.partitionList.find { it.partitionNum == partitionNum }
	}

	/**
	 * true: Extract leader partitions
	 * false: Extract replica partitions
	**/
	private List<Partition> extractSubPartitionList(String brokerId, Boolean leaderPartitions) {
		List<Partition> subPartitionList = []

		this.partitionList.each { partition ->
			if (leaderPartitions) {
				//Extract leader partitions
				if (partition.getLeader().getBrokerId() == brokerId) {
					subPartitionList << partition
				}
			} else {
				//Extract replica partitions
				if (partition.getLeader().getBrokerId() != brokerId) {
					subPartitionList << partition
				}
			}
		}
		//Sort the sub-partition replica list in order to put in front those which have more leads
		PartitionAnalyzer pa = PartitionAnalyzer.getInstance()

		subPartitionList.sort { p ->
			Integer avg = 0
			p.getReplicaList().each { rep ->
    			avg += pa.getPartitionLeaderCount(rep.getBroker().getBrokerId(), this.topicName)
    		}
    		avg /= p.getReplicaList().size()
    	}
    	subPartitionList = subPartitionList.reverse()

		subPartitionList
	}

	public void relocatePartitions(Broker brokerSrc, Integer partitionAvg) {
		PartitionAnalyzer pa = PartitionAnalyzer.getInstance()
		//Number of partitions owned by the broker source
		Integer ownedPartition = this.getPartitionCounter()
		//Get the number of partitions on which the broker source is leader
		Integer brokerSrcLeaderCount = pa.getPartitionLeaderCount(brokerSrc.getBrokerId(), this.topicName)
		
		//Find a broker for reallocation
		Broker brokerDest = pa.findDestinationBroker(this.topicName, partitionAvg)
		//Get the number of partitions on which the broker dest is leader
		Integer brokerDestLeaderCount = pa.getPartitionLeaderCount(brokerDest.getBrokerId(), this.topicName)
		
		def rangeToReassign = 0..<ownedPartition - partitionAvg

		Iterator partitionLeaderIterator = this.extractSubPartitionList(brokerSrc.getBrokerId(), true).iterator()
		Iterator partitionReplicaIterator = this.extractSubPartitionList(brokerSrc.getBrokerId(), false).iterator()

		rangeToReassign.each {
			if (partitionReplicaIterator.hasNext() || partitionLeaderIterator.hasNext()) {
				//TODO prioritize a partition with leadership if a gap exists between brokerSrc and brokerDst
				Partition partitionToRelocate = null

				//Switch a leader partition if:
				//	- One is available AND
				//	- The brokerDest needs to become leader AND
				//	- The number of leader partitions on the brokerSrc are greater than the average
				if (brokerSrcLeaderCount > partitionAvg && brokerDestLeaderCount <= partitionAvg && brokerSrcLeaderCount > brokerDestLeaderCount) {
					Partition partition = null
					while (partitionToRelocate == null && partitionLeaderIterator.hasNext()) {
						partition = partitionLeaderIterator.next()
						//Found one partition with leader to relocate
						if (!this.isLockedPartition(partition.getPartitionNum())) {
							partitionToRelocate = partition
						}
					}
				}
				//I no leader partitions available rebalance a replica
				if (partitionToRelocate == null) {
					//Find an non locked partition
					Partition partition = null
					while (partitionToRelocate == null && partitionReplicaIterator.hasNext()) {
						partition = partitionReplicaIterator.next()
						//Found one partition with leader to relocate
						if (!this.isLockedPartition(partition.getPartitionNum())) {
							partitionToRelocate = partition
						}
					}
				}

				//A partition to relocate was found, the relocation process can start
				if (partitionToRelocate != null) {
					//Adjust the number of leader partitions
					brokerSrcLeaderCount--
					brokerDestLeaderCount++

					List<Replica> replicaList = partitionToRelocate.getReplicaList()
					//Extract the the replica which is gonna be relocated
					Replica relocatedReplica = replicaList.find { it.getBroker().getBrokerId() == brokerSrc.getBrokerId() }
					Boolean switchLeader = replicaList.findIndexOf { it == relocatedReplica } == 0

					//Reassign the leader as the switch occurs on the leader
					if (switchLeader) {
						partitionToRelocate.setLeader(brokerDest)
					}

					//Extract the brokers that replicate the same partition
					List<Replica> reminentRep = replicaList.findAll { it.getBroker().getBrokerId() != brokerSrc.getBrokerId() }

					//Disable the relocation for the previously selected partition on other brokers
					reminentRep.each { replica ->
						//Lock the partition for the current topic
						Topic reminentBrokerTopic = replica.getBroker().getTopic(this.topicName)
						reminentBrokerTopic.lockPartition(partitionToRelocate.getPartitionNum())

						if (switchLeader) {
							reminentBrokerTopic.getPartition(partitionToRelocate.getPartitionNum()).setLeader(brokerDest)
						}
						
						//Update reminent replicas to notify that brokerSrc is not in charge of the partition anymore
						//Get the current replica list
						List<Replica> reminentBrokerReplicaList = reminentBrokerTopic.getPartition(partitionToRelocate.getPartitionNum()).getReplicaList()
						//Search the broker that has relocated
						Replica reminentRelocatedReplica = reminentBrokerReplicaList.find { it.getBroker().getBrokerId() == brokerSrc.getBrokerId() }
						reminentRelocatedReplica.setBroker(brokerDest)
					}

					//Modify the replica to assign it to the new broker
					relocatedReplica.setIsReassigned(true)
					relocatedReplica.setBroker(brokerDest)
				}
			}
		}
	}

	public void lockPartition(String partitionNum) {
		if (!this.partitionLock.contains(partitionNum)){
			this.partitionLock << partitionNum
		}
	}

	public Boolean isLockedPartition(String partitionNum) {
		this.partitionLock.contains(partitionNum)
	}

	public void clearPartitionLock() {
		this.partitionLock.clear()
	}
	
	//////////////////////////////////////////////////////////////////
	//GETTERS
	
	public String getTopicName() {
		this.topicName
	}

	public Integer getPartitionCounter() {
		this.partitionList.size()
	}

	public List<Partition> getPartitionList() {
		this.partitionList
	}
}


/**
 *
*/
class Partition {
	private String partitionNum
	private List<Replica> replicaList
	private Broker leader

	public Partition (String partitionNum, List replicaList) {
		this.partitionNum = (partitionNum.length() > 1 ? partitionNum : "0" + partitionNum)
		this.replicaList = []
		replicaList.eachWithIndex { replicaBkId, i ->
			if (i == 0) {
				this.leader = PartitionAnalyzer.getInstance().searchBroker(String.valueOf(replicaBkId))
			}
			this.replicaList << new Replica(String.valueOf(replicaBkId))
		}
	}

	public calculateBrokerDependencies(String topicName) {
		PartitionAnalyzer pa = PartitionAnalyzer.getInstance()
		Integer avg = 0

		this.replicaList.each { rep ->
			avg += pa.getPartitionLeaderCount(rep.getBroker().getBrokerId(), topicName)
		}
		avg /= this.replicaList.size()
	}

	//////////////////////////////////////////////////////////////////
	//GETTERS

	public Broker getLeader() {
		this.leader
	}


	public void setLeader(Broker broker) {
		this.leader = broker
	}

	public String getPartitionNum() {
		this.partitionNum
	}

	public List<Replica> getReplicaList() {
		this.replicaList
	}
}

/**
 *
*/
class Replica {
	private Broker broker
	private Boolean isReassigned

	Replica(String brokerId) {
		this.broker = PartitionAnalyzer.getInstance().searchBroker(brokerId)
		this.isReassigned = false
	}

	public void setIsReassigned(Boolean isReassigned) {
		this.isReassigned = isReassigned
	}	

	public void setBroker(Broker newBroker) {
		this.broker = newBroker
	}
	//////////////////////////////////////////////////////////////////
	//GETTERS
	public Broker getBroker() {
		this.broker
	}

	public Boolean getIsReassigned() {
		this.isReassigned
	}
}


//////////////////////////////////////////////////////////////////////
// Command line options
//////////////////////////////////////////////////////////////////////

def options = new Options()
options.addOption("topicFilter", true, "Topics to filter (CSV list)")
options.addOption("partitionFilter", true, "Partition to filter (CSV list)")
//Actions
options.addOption("list", false, "List the current partition assignment")
options.addOption("rebalance", false, "Find a new partition assignment plan")
//I/O
options.addOption("generateJson", true, "Generate the Json partition assignment plan")
options.addOption("inputJson", true, "Json input containng the partition assignment plan")

CommandLineParser parser = new PosixParser()
CommandLine cmd = parser.parse(options, args)

//Extract filters
TOPIC_FILTERS = []
if (cmd.hasOption("topicFilter")) {
    TOPIC_FILTERS = cmd.getOptionValue("topicFilter").split(',')
}

PARTITION_FILTERS = []
if (cmd.hasOption("partitionFilter")) {
    PARTITION_FILTERS = cmd.getOptionValue("partitionFilter").split(',')
}

//List of action to execute
ACTIONS = []
if (cmd.hasOption("list")) {
	ACTIONS << "LIST"
}
if (cmd.hasOption("rebalance")) {
	ACTIONS << "REBALANCE"
}
//Exits if no actions selected
if (!ACTIONS.size()) {
	usage()
	System.exit(1)
}

JSON_INPUT = null
JSON_OUPUT = null

//I/O
if (cmd.hasOption("generateJson")) {
	ACTIONS << "GENERATE_JSON"
	JSON_OUPUT = cmd.getOptionValue("generateJson")
}
if (cmd.hasOption("inputJson")) {
	JSON_INPUT = cmd.getOptionValue("inputJson")
} else {
	usage()
	System.exit(2)
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

//////////////////////////////////////////////////////////////////////
//MAIN
//////////////////////////////////////////////////////////////////////

LOGGER.info(">> Start analyze.")

if (TOPIC_FILTERS.size()) {
	LOGGER.info("> Topic filters applied: ${TOPIC_FILTERS}")
} else {	
	LOGGER.info("> Any topic filters applied. Searching for all topics")
}

if (PARTITION_FILTERS.size()) {
	LOGGER.info("> Partition filters applied: ${PARTITION_FILTERS}")
} else {
	LOGGER.info("> Any partition filters applied. Searching for all partitions")
}

LOGGER.logIfThrows {

	//Get the JSON containing the current partition replica assignment plan
 	def slurper = new JsonSlurper()
 	def jsonCurrentReplicaAssignment = slurper.parse(new File(JSON_INPUT))
	
	PartitionAnalyzer pa = PartitionAnalyzer.getInstance()
	List<Broker> brokerList = pa.getBrokerList()

	//Generate the complex map that assign each broker with its topics and its partitions	
 	jsonCurrentReplicaAssignment.partitions.each { partition ->
 		String topicName = new String(partition.topic)
 		String partitionName = String.valueOf(partition.partition)
 		List<String> replicaList = partition.replicas
 		
 		//Iterate on the kafka broker list
 		brokerList.each { Broker broker ->
 			
 			//Proceed if no filters applied (size=0) or if the topic is part of the filters
			if ( (TOPIC_FILTERS.size() == 0) || TOPIC_FILTERS.contains("${partition.topic}") ) {
				//If a no partition filters are defined of if the partition is part of the filters
				if ( (PARTITION_FILTERS.size() == 0) || PARTITION_FILTERS.contains("${partition.partition}") ) {
		 			
		 			//Is the broker owner of this topic partition?
		 			if (partition.replicas.contains(Integer.parseInt(broker.getBrokerId()))) {

		 				//Does it already have registerd this topic
	 					if (broker.topicExists(partition.topic)) {
							Topic topic = broker.getTopic(topicName)
							topic.addPartition(partitionName, replicaList)
	 					} else {
							broker.addTopicWithPartitionsAndReplicas(topicName, partitionName, replicaList)
	 					}
		 			}
		 		}
	 		}
 		}
 	}

	if ('REBALANCE' in ACTIONS) {
		generateNewPartitionAssignment()
 		listCurrentPartitionAssignment()
	} else {
		if ('LIST' in ACTIONS) {
	 		listCurrentPartitionAssignment()
		}
	}

	if ('GENERATE_JSON' in ACTIONS) {
		generateJsonFile()
	}
}


def usage() {
    LOGGER.error("Usage: com.gumgum.kafka.KafkaPartitionAnalyzer.groovy --inputJson JSON_INPUT_PATH [--topicFilter] [--partitionFilter] --list|--rebalance|--generate-json JSON_OUTPUT_PATH")
}


def listCurrentPartitionAssignment() {
	PartitionAnalyzer pa = PartitionAnalyzer.getInstance()
	List<Broker> brokerList = pa.getBrokerList()

	brokerList.each { broker ->
		LOGGER.info(ANSI_YELLOW + "+--- ${broker.getTagName()} (${broker.getTopicCounter()} topic(s))" + ANSI_RESET)
 		broker.getTopicList().each { topic ->
			LOGGER.info(ANSI_CYAN + "|\t+--- ${topic.getTopicName()} (${topic.getPartitionCounter()} partition(s)) - The broker is leader on ${pa.getPartitionLeaderCount(broker.getBrokerId(), topic.getTopicName())} partitions" + ANSI_RESET)
			topic.getPartitionList().each { partition ->
				LOGGER.info(ANSI_PURPLE + "|\t|\t+--- ${partition.getPartitionNum()} - #${partition.getLeader().getTagName()} | replica leader avg : ${partition.calculateBrokerDependencies(topic.getTopicName())}" + ANSI_RESET)
				displayReplica = []
				partition.getReplicaList().each { replica ->
					if (replica.getIsReassigned()) {
						displayReplica << ANSI_GREEN + replica.getBroker().getTagName()	+ ANSI_BLUE
					} else {
						displayReplica << replica.getBroker().getTagName()
					}
				}
				if (topic.isLockedPartition(partition.getPartitionNum())) {
					LOGGER.info(ANSI_WHITE + "|\t|\t|\t+--- ${displayReplica}" + ANSI_RESET)
				} else {
					LOGGER.info(ANSI_BLUE + "|\t|\t|\t+--- ${displayReplica}" + ANSI_RESET)
				}
			}
 		}
	}
}

def generateNewPartitionAssignment() {
	PartitionAnalyzer pa = PartitionAnalyzer.getInstance()
	
	List<Broker> brokerList = pa.getBrokerList()
	
	totalPartitions = pa.getClusterOwnedPartitionNumber()
	totalTopics = pa.getClusterOwnedTopicNumber()
	brokerCount = brokerList.size()
	Integer partitionAvg = ((totalPartitions / brokerCount) / totalTopics).doubleValue().round()
	
	//Relocating partitions for each brokers
	brokerList.each { broker ->
		//Relocating partitions for each topics
		broker.getTopicList().each { topic ->
			ownedPartitionNum = topic.getPartitionCounter()
			if (ownedPartitionNum >= partitionAvg) {
				if (ownedPartitionNum - partitionAvg >= 3) {
					//Force Kafka's with big ecart type to send one more partition
					topic.relocatePartitions(broker, partitionAvg - 1)
				} else {
					topic.relocatePartitions(broker, partitionAvg)
				}
			}
		}
	}
}

def generateJsonFile() {
	def partitions = [:]

	PartitionAnalyzer pa = PartitionAnalyzer.getInstance()	
	List<Broker> brokerList = pa.getBrokerList()

	brokerList.each { broker ->
		broker.getTopicList().each { topic ->
			topic.getPartitionList().each { partition ->
				replicaBrokerList = []
				partition.getReplicaList().each { replica ->
					replicaBrokerList << Integer.parseInt(replica.getBroker().getBrokerId())
				}
				def newPartition = [
					"topic": "${topic.getTopicName()}",
					"partition": Integer.parseInt(partition.getPartitionNum()),
					"replicas": replicaBrokerList
				]
				
				if (!partitions.containsKey("${topic.getTopicName()}-${partition.getPartitionNum()}")) {
					partitions.put("${topic.getTopicName()}-${partition.getPartitionNum()}", newPartition)
				}
			}
		}
	}

	def data = [
  		"version": 1,
  		"partitions": partitions.collect { k,v -> v}
	]

	def jsonFinal = new groovy.json.JsonBuilder(data)

	def kafkaReassignmentPlan = new File(JSON_OUPUT)
	kafkaReassignmentPlan.withWriter { out ->
		out.writeLine(jsonFinal.toString())
	}
}