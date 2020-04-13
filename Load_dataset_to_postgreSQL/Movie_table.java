//package Big_data;

// @author : Chaitanya

//Contains sql queries to Create and insert into table Movie

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class Movie_table {
	public static HashMap<Integer, Integer> Movie_Map = new HashMap<>();

	@SuppressWarnings("resource")
	public static void main(String args[]) throws FileNotFoundException, IOException, SQLException {
		HashMap<Integer, Double> Rating_Mapx = new HashMap<>();
		HashMap<Integer, Double> Number_Map = new HashMap<>();
		
//		Rating_Map R = new Rating_Map();
		Rating_Map.main(args);
		
		Number_Map = Rating_Map.Number_Map;
		Rating_Mapx = Rating_Map.Rating_Map;
		
		Connection c = null;
		PreparedStatement preparedStatement = null;
		PreparedStatement preparedStatementGenre = null;
		PreparedStatement preparedStatementHas_Genre = null;
		String Create_table_Movie = "CREATE TABLE  MOVIE " + "(Movie_ID INT PRIMARY KEY     NOT NULL,"
				+ " Movie_Name           TEXT   , " + " Rating       FLOAT, " + " No_of_Votes        FLOAT)";

		String Create_table_Genre = "CREATE TABLE Genre " + "(Genre_ID INT PRIMARY KEY     NOT NULL,"
				+ " Genre_type_one TEXT, "+"Genre_type_two TEXT, "+ "Genre_type_three TEXT )";
		
		String Create_table_Has_Genre = "CREATE TABLE Has_Genre " + "(MovieGenre_ID INT, "
		+ "Genre_name TEXT,"+"FOREIGN KEY(MovieGenre_ID) references MOVIE(Movie_ID))";
		
		try {
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Practise", "postgres", "password");
		//c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/"+Acted_in.dbName, "postgres", Acted_in.password);

			preparedStatement = c.prepareStatement(Create_table_Movie);
			//preparedStatementGenre = c.prepareStatement(Create_table_Genre);
			{

				 preparedStatement.execute();
				 preparedStatementHas_Genre = c.prepareStatement(Create_table_Has_Genre);;
				 preparedStatementHas_Genre.execute();

			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");

		//String filename = "C:\\Users\\13155\\Desktop\\Chaitanya\\Sem2\\BigData\\title.basics.tsv.gz\\";
		String filename = "title.basics.tsv.gz";
		BufferedReader br = null;

		br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
		
		int count = 0;
		int count2 = 0;
		
		try {
			preparedStatement = c.prepareStatement(
					"INSERT INTO MOVIE (Movie_ID,Movie_Name,Rating,No_of_Votes) " + "VALUES (?,?,?,?);");
			
			//preparedStatementGenre = c.prepareStatement("INSERT INTO Genre (Genre_ID, Genre_type_one,Genre_type_two,Genre_type_three) " + "VALUES (?,?,?,?);");
		
			preparedStatementHas_Genre = c.prepareStatement("INSERT INTO Has_Genre (MovieGenre_ID, Genre_name) " + "VALUES (?,?);");
			
		} catch (SQLException e1) {
			System.out.println("Problem in Insert statements");
			e1.printStackTrace();
			System.exit(0);
		}
		int Genre_Id_Count = 0;
		while (br.ready()) {

			if (count == 0) {
				count += 1;
				br.readLine();
				continue;

			}
			String Row = br.readLine();
			String RowStore[] = Row.split("\t");
			String Movie = RowStore[0].substring(2, RowStore[0].length());
			System.out.println(Movie);
			int Movie_primary_key = Integer.parseInt(Movie);
			// String Movie_alpha = Movie_primary_key.substring(0,2);
			
			//put values in a map so that they are accessible in the other table
			Movie_Map.put(Movie_primary_key , Movie_primary_key );
			
			String Movie_name = RowStore[2];
			String Is_adult = RowStore[4];
			String Genre[] = RowStore[8].split(",");
			
			//Pattern Matching to check if the cell has "\N" in it
			Pattern p = Pattern.compile("[^A-Za-z0-9]");
			
		     Matcher m = p.matcher(Genre[0]);
		     boolean b = m.find();
		  
		    
			System.out.println(Movie_primary_key + " " + Movie_name);
			
			//skip row if movie is adult
			if (Is_adult == "1") {
				continue;
			}
			
			
			try {

				preparedStatement.setLong(1, Movie_primary_key);
				preparedStatement.setString(2, Movie_name);
				//Check if this Movie_ID is there in the Ratings
				if(Rating_Mapx.containsKey(Movie_primary_key)) {
					preparedStatement.setDouble(3, Rating_Mapx.get(Movie_primary_key));
				}
				else {
					preparedStatement.setDouble(3, -1);
					
				}
				//Check if this Movie_ID is there in the Ratings
				if(Number_Map.containsKey(Movie_primary_key)) {
					preparedStatement.setDouble(4, Number_Map.get(Movie_primary_key));
				}
				else {
					preparedStatement.setDouble(4, -1);
					
				}
				
				preparedStatement.addBatch();

				Genre_Id_Count += 1;
				
				/*
				 * This is making of the Has_Genre Table
				 */
					for(int i=0;i<Genre.length;i++) {
						if(b) {
							continue;
						}
						preparedStatementHas_Genre.setLong(1, Movie_primary_key);
						preparedStatementHas_Genre.setString(2, Genre[i]);
						preparedStatementHas_Genre.addBatch();

					}
				
				//Counter for checking the batch size
				if (count2++ == 500) {
					count2 = 0;
					preparedStatement.executeBatch();
					//preparedStatementGenre.executeBatch();
					preparedStatementHas_Genre.executeBatch();
					//break;

				}

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Error in inserting values to Movie database");
				e.printStackTrace();
				System.exit(0);
			}

		}

		try {
			
		}
		finally {
			preparedStatement.executeBatch();

			preparedStatementHas_Genre.executeBatch();
			System.out.println("DONE");
			preparedStatement.close();
			//preparedStatementGenre.close();
			preparedStatementHas_Genre.close();
			c.close();
		}

	}

}
