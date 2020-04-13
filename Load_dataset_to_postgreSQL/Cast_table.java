//package Big_data;


// @author : Chaitanya

//Contains sql queries to Create and insert into table : member

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

public class Cast_table {
	
	/*
	 * Hashmap to make sure no repeated values are checked
	 */
	
	 public static HashMap<Integer,Integer> Cast_map = new HashMap<>();
	 
	/*
	 * Using Names.basics.tsv because on observation it seems like there are consistant name_ids
	 */
	@SuppressWarnings("resource")
	public static void main(String args[]) throws FileNotFoundException, IOException, SQLException {
		BufferedReader br = null;
		//String filename = "C:\\Users\\13155\\Desktop\\Chaitanya\\Sem2\\BigData\\name.basics.tsv.gz\\";
		String filename = "name.basics.tsv.gz";

		br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
		int count = 0;
		
		 
		/*
		 * Connections
		 */
		Connection c = null;
		PreparedStatement preparedStatementCast = null;
		try {
			
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Practise", "postgres", "password");
			//c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/"+Acted_in.dbName, "postgres", Acted_in.password);

			
		} catch (SQLException e) {
			
			System.out.println("Error in connecting");
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("connected");
		/*
		 * Creating Table
		 */
		
		
		
		String Create_table_Genre = "CREATE TABLE  member " + "(member_ID INT PRIMARY KEY     NOT NULL,"
				+ " member_name TEXT )";
		//String Create_table_Cast = "CREATE TABLE member " + "(Name_ID INT PRIMARY KEY     NOT NULL,"
			//	+ " Cast_Name           TEXT   ) "; 
		try {
		preparedStatementCast = c.prepareStatement(Create_table_Genre);
		
		preparedStatementCast.execute();
		
		/*
		 * Insert into table, dont forget to set, addbatch and then execute
		 */
		preparedStatementCast = c.prepareStatement(
				"INSERT INTO member (member_ID,member_name) " + "VALUES (?,?);");
		
	}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int count2 = 0;
		try {
		while (br.ready()) {

			/*
			 * Skip first line since it consist the column names
			 */
			if (count == 0) {
				count += 1;
				System.out.println(br.readLine());
				
				continue;

			}
			
			/*
			 * Reading each tuple
			 */
			String Row = br.readLine();
			String RowStore[] = Row.split("\t");
			String Cast_prim = RowStore[0].substring(2, RowStore[0].length());
			int Cast_primary_key = Integer.parseInt(Cast_prim);
			String Cast_name = RowStore[1];
			
			//So that we do not repeat IDs
			if(Cast_map.containsKey(Cast_primary_key)) {
				continue;
			}
			Cast_map.put(Cast_primary_key, Cast_primary_key);

			preparedStatementCast.setLong(1, Cast_primary_key);
			preparedStatementCast.setString(2, Cast_name);
			preparedStatementCast.addBatch();

			
			
			if(count2++==500) {
				preparedStatementCast.executeBatch();
				count2=0;
			}
			
			
		}
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
		finally {
		preparedStatementCast.executeBatch();
		preparedStatementCast.close();
		c.close();
		}
	}
}
