package com.vamsi.wordcloud;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DiagramProducer {

   KafkaProducer<String, String> producer;
   
   public DiagramProducer() throws IOException
   {
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");    
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);  
      props.put("linger.ms", 1);  
      props.put("buffer.memory", 33554432);
      props.put("auto.commit.interval.ms", 1000);
      props.put("block.on.buffer.full", true);
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");  
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");

      producer = new KafkaProducer<>(props);

   }
   
   public void kafkaWrite(String url, String message) throws Exception 
   {
      
      System.out.println("Writing to Kafka");     
      producer.send(new ProducerRecord<String, String>( "productDescriptions", 
      url, message));
      
      System.out.println("Message sent successfully");
      
      producer.close();
   }


}