//package Big_data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class index_q3 {
	
	public static String dbName;
	public static String password;
	public static String port;
	public static String table;
	static Connection c = null;
	public static void setConnection() throws SQLException {
		
		Scanner sc = new Scanner(System.in);
		
		System.out.println("Please enter your port number");
		port= sc.nextLine();
		
		System.out.println("Please enter your database name");
		dbName = sc.nextLine();
		
		System.out.println("Please enter your table ");
		table = sc.nextLine();
		
		System.out.println("Please enter your database password");
		password = sc.nextLine();
		
		if(port.equals("")) {
			port = "5432";
		}
		if(dbName.equals("")) {
			dbName = "prac";
		}
		if(password.equals("")) {
			password = "password";
		}
		if(table.equals("")) {
			table = "final";
		}
		

		
	}
	
	public static void main(String args[]) throws FileNotFoundException, IOException, SQLException {
		
		setConnection();
		
		
		System.out.println("------------------------------------------");
		System.out.println("Question 3");
		System.out.println("------------------------------------------");
		Normalize.main(args);
		
	}

}
