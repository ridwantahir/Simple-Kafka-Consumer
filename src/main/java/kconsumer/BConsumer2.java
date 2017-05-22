package kconsumer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

public class BConsumer2 extends Thread{
	KafkaConsumer<Integer, String> consumer;
	List<String> topic;
	String groupId;
	String consumerID;
	public BConsumer2(List<String> topic, String groupID, String consumerID) {
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
	    props.put("enable.auto.commit", "false");
	    //props.put("auto.commit.interval.ms", "1000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    this.consumer = new KafkaConsumer<>(props);
	    consumer.subscribe(topic, new RebalanceListener());
	}
	@Override
	public void run(){
		try{
			while(true){
				ConsumerRecords<Integer, String> records=consumer.poll(100);
				if(records.isEmpty())System.out.println("empty");
				for(TopicPartition par:records.partitions()){
					List<ConsumerRecord<Integer, String>> precords=records.records(par);
					for(ConsumerRecord<Integer, String> record:precords){
						System.out.println("Partition"+record.partition()+"  offset:"+record.offset()+" message: "+record.key()+":"+record.value());
					}
					Long lastoffset=precords.get(precords.size()-1).offset();
					consumer.commitSync(Collections.singletonMap(par, new OffsetAndMetadata(lastoffset + 1)));
					/*consumer.commitAsync(new OffsetCommitCallback() {
					      @Override
					      public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, 
					                             Exception exception) {
					        if (exception != null) {
					          // application specific failure handling
					        }
					      }
					    });*/
				}
				
			}
		}
		catch(CommitFailedException e){
			
		}
		finally{
			consumer.wakeup();
		}
	}
	/*@Override
	public void run(){
		int count=0;
		
		try{
			while(true){
				if(this.consumerID.equals("dying")){if(count>=1);}
				ConsumerRecords<Integer, String> records=consumer.poll(100);
				if(records.isEmpty())System.out.println("empty");
				for(ConsumerRecord<Integer, String> record:records){
					System.out.println("Partition"+record.partition()+"  offset:"+record.offset()+" message: "+record.key()+":"+record.value());
				}
				consumer.commitSync();
				count++;
			}
		}
		catch(CommitFailedException  e){
			
		}
		finally{
			consumer.close();
		}
	}*/
	 public void shutdown() {
		    consumer.wakeup();
		  }

}
