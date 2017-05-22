package kconsumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener implements ConsumerRebalanceListener{

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> arg0) {
		System.out.println(arg0+" partitions_assigned");
		
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
		System.out.println(arg0+" partitions_revoked");
		
	}

}
