package kconsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class BConsumer3 extends Thread
{
	KafkaConsumer<Integer, String> consumer;
	List<String> topic;
	String groupId;
	String consumerID;
	int mpartition;
	public BConsumer3(List<String> topic, String groupID, String consumerID, int mpartition) {
		this.mpartition=mpartition;
		this.topic=topic;
		this.consumerID=consumerID;
		this.groupId=groupID;
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", groupId);
	    props.put("auto.offset.reset", "earliest");
	    props.put(" session.timeout.ms", "10000");
	    props.put("max.poll.interval.ms", "10000");
	    props.put("max.poll.records", "100");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    this.consumer = new KafkaConsumer<>(props);
	   // consumer.subscribe(topic, new RebalanceListener());
	    List<TopicPartition> alltop=new ArrayList<>();
	    for(String t:topic){
	    	TopicPartition partit=new TopicPartition(t,this.mpartition);
	    	alltop.add(partit);
	    }
	    consumer.assign(alltop);
	    
	}
	@Override
	public void run(){
		//consumer.seekToBeginning(consumer.assignment());// consumer is not assigned partition until call to the first poll()
		
		int count=0;
		
		try{
			while(true){
				if(this.consumerID.equals("dying")){if(count>=1);}
				ConsumerRecords<Integer, String> records=consumer.poll(100);
				if(records.isEmpty())System.out.println("empty");
				for(ConsumerRecord<Integer, String> record:records){
					System.out.println("Partition"+record.partition()+"  offset:"+record.offset()+" message: "+record.key()+":"+record.value());
				}
				count++;
			}
		}
		catch(WakeupException  e){
			
		}
		finally{
			consumer.close();
		}
	}
	 public void shutdown() {
		    consumer.wakeup();
		  }
}
