package kconsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
	public static void main(String[] args){
		  int numConsumers = 3;
		  String groupId = "ugpyufsr";
		  List<String> topics = Arrays.asList("javatopic2");
		  ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
		  final List<BConsumer3> consumers = new ArrayList<>();
		  BConsumer3 c1=new BConsumer3(topics, groupId, 0+"",0);		  
		  consumers.add(c1);
		  executor.submit(c1);
		  
		  BConsumer3 c2=new BConsumer3(topics, groupId, "dying",1);		  
		  consumers.add(c2);
		  executor.submit(c2);
		  
		  BConsumer3 c3=new BConsumer3(topics, groupId, 1+"",2);		  
		  consumers.add(c3);
		  executor.submit(c3);

		  Runtime.getRuntime().addShutdownHook(new Thread() {
		    @Override
		    public void run() {
		      for (BConsumer3 consumer : consumers) {
		        consumer.shutdown();
		      } 
		      executor.shutdown();
		      try {
		        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
		      } catch (InterruptedException e) {
		        e.printStackTrace();
		      }
		    }
		  });
	}
}
