//package Big_data;

// @author : Chaitanya

//Contains sql queries to Create and insert into table Acted_in , writer, director and producer


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
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class Acted_in {
	/*
	 * Checking in principal table if the person has acted. Check if that tconst is
	 * there in the movie table map or not
	 */
	static String dbName;
	static String password;


	@SuppressWarnings("resource")
	public static void main(String args[]) throws FileNotFoundException, IOException, SQLException {
		BufferedReader br = null;
		//String filename = "C:\\Users\\13155\\Desktop\\Chaitanya\\Sem2\\BigData\\title.principals.tsv.gz";
		String filename = "title.principals.tsv.gz";
		
		
		//if (args.length > 0) {
		  //  try {
		    	
		    //    dbName = args[0];
		      //  password = args[1];
		        
		    //} catch (NumberFormatException e) {
		   // 	System.out.println("Please enter dbname and password");
		   //     System.exit(1);
		   // }
		
		br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
		// br = new BufferedReader(new InputStreamReader(new
		// FileInputStream(filename)));

		int count = 0;
		HashMap<Integer, Integer> Movie_Map_store = new HashMap<>();
		HashMap<Integer, Integer> Name_Map_store = new HashMap<>();

		/*
		 * Storing movie map and Name to compare
		 */
		Movie_table.main(args);
		Cast_table.main(args);
		Movie_Map_store = Movie_table.Movie_Map;
		Name_Map_store = Cast_table.Cast_map;

		/*
		 * Creating, connecting and inserting in a table
		 * 
		 */

		Connection c = null;
		PreparedStatement preparedStatementActed_in = null;
		PreparedStatement preparedStatementWrite = null;
		PreparedStatement preparedStatementDirect = null;
		PreparedStatement preparedStatementProduce = null;

		c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Practise", "postgres", "password");


		//c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/"+dbName, "postgres", password);

		String Create_table_Acted_in = "CREATE TABLE  Acted_in " + "(MovieActed_ID INT ,"

				+ " Name_ID INT,"

				+ "FOREIGN KEY(MovieActed_ID) references MOVIE(Movie_ID),"

				+ "FOREIGN KEY(Name_ID) references member(member_ID))";

		preparedStatementActed_in = c.prepareStatement(Create_table_Acted_in);

		preparedStatementActed_in.execute();
		preparedStatementActed_in = c.prepareStatement("INSERT INTO Acted_in(MovieActed_ID, Name_ID) values (?,?);");

		// preparedStatementActed_in = c.prepareStatement("INSERT INTO
		// Acted_in(MovieActed_ID, Name_ID)"+
		// "SELECT Movie.Movie_ID, member.member_ID FROM Movie, member WHERE
		// Movie.Movie_ID=? and member.member_ID=?");

		// “INSERT IGNORE INTO X(a, b) SELECT A.id, B.id FROM A, B WHERE A.id=? AND
		// B.id=?
		/*
		 * The writer table
		 */
		String Create_table_Write = "CREATE TABLE Write " + "(written_id  INT ,"

				+ " Name_ID INT,"

				+ "FOREIGN KEY(written_id) references MOVIE(Movie_ID),"

				+ "FOREIGN KEY(Name_ID) references member(member_ID))";
		preparedStatementWrite = c.prepareStatement(Create_table_Write);

		preparedStatementWrite.execute();

		preparedStatementWrite = c.prepareStatement("INSERT INTO Write(written_id, Name_ID) values (?,?);");

		// preparedStatementWrite = c.prepareStatement("INSERT INTO Write(MovieActed_ID,
		// Name_ID)"+
		// "SELECT Movie.Movie_ID, member.member_ID FROM Movie, member WHERE
		// Movie.Movie_ID=? and member.member_ID=?");
		/*
		 * The producer table
		 */
		String create_table_Produce = "CREATE TABLE produce " + "( producer_id  INT ,"

				+ " Name_ID INT,"

				+ "FOREIGN KEY(producer_id) references MOVIE(Movie_ID),"

				+ "FOREIGN KEY(Name_ID) references member(member_ID))";

		preparedStatementProduce = c.prepareStatement(create_table_Produce);

		preparedStatementProduce.execute();
		preparedStatementProduce = c.prepareStatement("INSERT INTO produce(producer_id, Name_ID) values (?,?); ");

		// preparedStatementProduce = c.prepareStatement("INSERT INTO
		// produce(MovieActed_ID, Name_ID)"+
		// "SELECT Movie.Movie_ID, member.member_ID FROM Movie, member WHERE
		// Movie.Movie_ID=? and member.member_ID=? ");
		// + "ON CONFLICT (Movie.Movie_ID,member.member_ID) DO NOTHING;");
		/*
		 * Director table
		 */
		String create_table_Direct = "CREATE TABLE direct " + "(directed_id  INT ,"

				+ " Name_ID INT,"

				+ "FOREIGN KEY(directed_id ) references MOVIE(Movie_ID),"

				+ "FOREIGN KEY(Name_ID) references member(member_ID))";

		preparedStatementDirect = c.prepareStatement(create_table_Direct);

		preparedStatementDirect.execute();
		preparedStatementDirect = c.prepareStatement("INSERT INTO direct(directed_id, Name_ID) values (?,?); ");
		// preparedStatementDirect = c.prepareStatement("INSERT INTO
		// direct(MovieActed_ID, Name_ID)"+
		// "SELECT Movie.Movie_ID, member.member_ID FROM Movie, member WHERE
		// Movie.Movie_ID=? and member.member_ID=?");
		// + " ON CONFLICT (Movie.Movie_ID,member.member_ID) DO NOTHING;");

		int countA = 0;
		int countD = 0;
		int countW = 0;
		int countP = 0;

		while (br.ready()) {

			if (count == 0) {
				count += 1;
				br.readLine();
				continue;

			}
			String Row = br.readLine();
			String RowStore[] = Row.split("\t");

			/*
			 * Get movie ID
			 */
			String Movie = RowStore[0].substring(2, RowStore[0].length());
			int Movie_primary_key = Integer.parseInt(Movie);

			/*
			 * Get name ID
			 */
			String Name = RowStore[2].substring(2, RowStore[2].length());
			System.out.println("this is Movie" + Movie);
			int Name_primary_key = Integer.parseInt(Name);

			String patternActor = "act.*";

			boolean is_act = Pattern.matches(patternActor, RowStore[3]);
			/*
			 * Checking if the person is an actor/actress and if the movie he/she is in
			 * corresponds to a movie in the movie table We do this because Movie_table has
			 * a foriegn key in Acted_in table
			 * 
			 */

			/*
			 * Checking if the person is an writer
			 */
			String patternWriter = "writer";
			boolean is_Writer = Pattern.matches(patternWriter, RowStore[3]);

			/*
			 * Checking if the person is an Director
			 */
			String patternDirects = "director";
			boolean is_Director = Pattern.matches(patternDirects, RowStore[3]);

			/*
			 * Checking if the person is an Producer
			 */
			String patternProduces = "producer";
			boolean is_producer = Pattern.matches(patternProduces, RowStore[3]);

			if (is_act && Movie_Map_store.containsKey(Movie_primary_key)
					&& Name_Map_store.containsKey(Name_primary_key)) {

				/*
				 * Add this value to the Acted_in table
				 */
				System.out.println(is_act);
				if (is_act) {
					preparedStatementActed_in.setLong(1, Movie_primary_key);
					preparedStatementActed_in.setLong(2, Name_primary_key);
					preparedStatementActed_in.addBatch();
					System.out.println("This is count   " + countA);

					if (countA++ == 500) {
						System.out.println("HERE");

						preparedStatementActed_in.executeBatch();
						countA = 0;

					}
				}

			}

			else if (is_Writer && Movie_Map_store.containsKey(Movie_primary_key)
					&& Name_Map_store.containsKey(Name_primary_key)) {

				if (is_Writer) {

					preparedStatementWrite.setLong(1, Movie_primary_key);
					preparedStatementWrite.setLong(2, Name_primary_key);
					preparedStatementWrite.addBatch();
					System.out.println(countW);

					if (countW++ == 500) {
						preparedStatementWrite.executeBatch();
						countW = 0;

					}

				}
			}

			else if (is_Director && Movie_Map_store.containsKey(Movie_primary_key)
					&& Name_Map_store.containsKey(Name_primary_key)) {

				if (is_Director) {
					preparedStatementDirect.setLong(1, Movie_primary_key);
					preparedStatementDirect.setLong(2, Name_primary_key);
					preparedStatementDirect.addBatch();
					System.out.println(countD);

					if (countD++ == 500) {
						preparedStatementDirect.executeBatch();
						countD = 0;

					}

				}
			}

				else if (is_producer && Movie_Map_store.containsKey(Movie_primary_key)
						&& Name_Map_store.containsKey(Name_primary_key)) {

					if (is_producer) {
						preparedStatementProduce.setLong(1, Movie_primary_key);
						preparedStatementProduce.setLong(2, Name_primary_key);
						preparedStatementProduce.addBatch();
						System.out.println(countP);

						if (countP++ == 500) {
							preparedStatementProduce.executeBatch();
							countP = 0;

						}

					}

				

			}
		}
		preparedStatementActed_in.executeBatch();
		preparedStatementWrite.executeBatch();
		preparedStatementDirect.executeBatch();
		preparedStatementProduce.executeBatch();
		preparedStatementActed_in.close();
		preparedStatementDirect.close();
		preparedStatementProduce.close();
		preparedStatementWrite.close();
		c.close();
		br.close();
		System.out.println("DONEEE");
	}
}
