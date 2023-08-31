package com.kafka.project.kafkaproducer;

import java.util.Scanner;
import java.io.IOException;
import java.sql.SQLException;


public class App 
{
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    	try (Scanner scanner = new Scanner(System.in)) {
			Operations op=new Operations();
			op.ConnectToDatabase();

			while(true){
				System.out.print("SQL command #:");
				String sql = scanner.nextLine();
				op.SQLCommand(sql);
				System.out.println();
				
			}
		}
    	
		
	}
    
}
