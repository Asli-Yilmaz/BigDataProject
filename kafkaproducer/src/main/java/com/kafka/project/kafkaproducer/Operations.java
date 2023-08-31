package com.kafka.project.kafkaproducer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Operations {
	Connection conn=this.ConnectToDatabase();

	public Connection ConnectToDatabase() {
		Connection conn=null;
		
		try {
			conn=DriverManager.getConnection("jdbc:postgresql://localhost:5432/eclassesdb","postgres","12345");
		if (conn != null)
            System.out.println();
        else
            System.out.println("Connection failed!");

	    } catch (SQLException throwables) {
	        throwables.printStackTrace();}
	    
	    return conn;
	}
	private void writeTOFile() throws SQLException, IOException {
		String query="SELECT * FROM eclasses";
		Statement statement = conn.createStatement();

        
        ResultSet resultSet = statement.executeQuery(query);

        
        String filePath = "data_file.txt";
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));

        
        while (resultSet.next()) {
            
        	boolean manufacturer_product_description=resultSet.getBoolean("manufacturer_product_description");
        	boolean link=resultSet.getBoolean("link");
        	boolean gln_of_supplier=resultSet.getBoolean("gln_of_supplier");
        	long id = resultSet.getLong("id");
        	boolean taric=resultSet.getBoolean("taric");
            boolean details=resultSet.getBoolean("details");
            boolean name_numberofmanufacturer_product_article=resultSet.getBoolean("name_numberofmanufacturer_product_article");
            boolean name_number_of_supplier=resultSet.getBoolean("name_number_of_supplier");
            boolean uri_glnofmanufacturer=resultSet.getBoolean("uri_glnofmanufacturer");
            boolean gtin=resultSet.getBoolean("gtin");
            boolean brand=resultSet.getBoolean("brand");                
            String description = resultSet.getString("description");
            long eclassnumber=resultSet.getLong("eclassnumber");
            String category=resultSet.getString("category");             
            
            
            
            writer.write(id + "," + description + ","  + category 
            		+ "," +manufacturer_product_description
            		+ "," + link + "," + gln_of_supplier
            		+ "," + taric+ "," +details
            		+ "," + name_numberofmanufacturer_product_article
            		+ "," + name_number_of_supplier
            		+ "," + uri_glnofmanufacturer
            		+ "," + gtin + ","+brand
            		+ "," + eclassnumber);
            writer.newLine();
        }

        writer.close();


        resultSet.close();
       
	}
	public void  SQLCommand(String sql) throws IOException {

        try {
        
            Statement stmt = conn.createStatement();
            if(sql.toLowerCase().contains("select")&& !sql.toLowerCase().contains("count")) {
            	 ResultSet resultSet = stmt.executeQuery(sql);
                 while (resultSet.next()) {
                     int id = resultSet.getInt("id");
                     String description = resultSet.getString("description");
                     boolean manufacturer_product_description=resultSet.getBoolean("manufacturer_product_description");
	                 boolean link=resultSet.getBoolean("link");
	                 boolean gln_of_supplier=resultSet.getBoolean("gln_of_supplier");
	                 boolean taric=resultSet.getBoolean("taric");
                     boolean details=resultSet.getBoolean("details");
                     boolean name_numberofmanufacturer_product_article=resultSet.getBoolean("name_numberofmanufacturer_product_article");
                     boolean name_number_of_supplier=resultSet.getBoolean("name_number_of_supplier");
                     boolean uri_glnofmanufacturer=resultSet.getBoolean("uri_glnofmanufacturer");
                     boolean gtin=resultSet.getBoolean("gtin");
                     boolean brand=resultSet.getBoolean("brand");                
                     int eclassnumber=resultSet.getInt("eclassnumber");
                     String category=resultSet.getString("category");
                     System.out.println("id: " + id + ", description: " + description + ", category: " + category+ ", Manufacturer prodcut desxription: " + manufacturer_product_description + ", Link: " + link + ", GLN Of Supplier: " + gln_of_supplier + ", TARIC: " + taric + ", Details: " + details + "Name/number of Manufacturer: " + name_numberofmanufacturer_product_article + ", Name/number of Supplier: " + name_number_of_supplier + ", URI/GLN of Manufacturer: " + uri_glnofmanufacturer + ", GTIN: " + gtin + ", Brand: " + brand + ", Eclassnumber: " + eclassnumber );
                     System.out.println();
                 }

                 resultSet.close();
            }
            /**if (sql.toLowerCase().contains("update")) {
            	stmt.executeUpdate(sql);
            }**/
            else stmt.executeUpdate(sql);
            System.out.println("****Command executed succesfully.****");
            
            
            writeTOFile();
            ConnectToKafka();

            
        } catch (SQLException throwables) {
        
            throwables.printStackTrace();
        }
    }
	private void ConnectToKafka() {
		 
		String bootstrapServers = "localhost:9092";
        String topicName = "new_topic";
        int numPartitions = 1;  

        
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
           
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Topic created: " + topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

		 Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	    
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

	        
	    	try (BufferedReader reader = new BufferedReader(new FileReader("data_file.txt"))) {
	            String line;
	            while ((line = reader.readLine()) != null) {
	                
	                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, line.trim());
	                producer.send(record);
	            }
	            System.out.println("Data had been sent to the Kafka.");
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            
	            producer.close();
	        }
	}
}
	
	
