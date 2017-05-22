package kconsumer;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
/*import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;*/

public class BConsumer extends Thread{
	KafkaConsumer<Integer, String> consumer;
	List<String> topic;
	String groupId;
	String consumerID;
	public BConsumer(List<String> topic, String groupID, String consumerID) {
		this.topic=topic;
		this.consumerID=consumerID;
		this.groupId=groupID;
		Properties props=new Properties();
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", groupId);
	    props.put("auto.offset.reset", "earliest");
	    props.put(" session.timeout.ms", "1000");
	    props.put("max.poll.interval.ms", "1000");
	    props.put("max.poll.records", "100");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    this.consumer = new KafkaConsumer<>(props);
	    consumer.subscribe(topic, new RebalanceListener());
	    
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
