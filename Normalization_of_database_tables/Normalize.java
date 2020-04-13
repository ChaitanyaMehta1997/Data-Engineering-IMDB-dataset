//package Big_data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class Normalize {

	String[] Store;
	String[] temp;
	
	/*
	Generate all combinations of columns in the table
	*/
	public static String[] Generate_combinations(String[] store) throws SQLException {

		ArrayList<String> FD = new ArrayList<>();
		HashMap<String, ArrayList<String>> BigMap = new HashMap<>();
		
		double limit = Math.pow(2, store.length);

		for (int i = 1; i < limit; i++) {

			ArrayList<String> Temp = new ArrayList<>();
			// System.out.println("------------------------");
			for (int j = 0; j < store.length; j++) {
				int index = 0;
				
				if ((i & (1 << j)) > 0) {

					
					Temp.add(store[j]);
					index += 1;
				}

			}		
		/*
			Pruning Step
		*/
			if (Temp.size() > 1) {
				// This is the modification
				if (Temp.size() <= 3) {
					boolean check = false;
					for (int q = 0; q < Temp.size() - 1; q++) {
					if(BigMap.containsKey(Temp.get(q))) {
						
						for(String N:BigMap.get(Temp.get(q))) {
							if(N.equals(Temp.get(Temp.size() - 1))) {
								// Means we have already checked it
								check = true;
								
							}
							
						}

						
					}
					}
					if(check) {
						//Dont check all these columns for FDs
						continue;
					}
					
					if (Functional_dep_checker(Temp)) {
						
						String BigLHS="";
						
						for (int z = 0; z < Temp.size() - 1; z++) {
							BigLHS = BigLHS + Temp.get(z);
							System.out.print(Temp.get(z) + ", ");

						}
						System.out.print("---> " + Temp.get(Temp.size() - 1));
						System.out.println("");
						
						ArrayList<String> x = new ArrayList<>();
						x.add(Temp.get(Temp.size() - 1));
						if(!BigMap.containsKey(BigLHS)) {
							BigMap.put(BigLHS, x);
						}
						else {
							ArrayList<String> x1 = new ArrayList<>();
							x1 = BigMap.get(BigLHS);
							x1.add(Temp.get(Temp.size()-1));
							BigMap.put(BigLHS, x1);
							
						}
						

					} else {

					

					}

				}
			}
		}

		
		System.out.println("");

		return store;
	}
	
	
	/*
	This method finds all the Functional dependencies in the table
	*/
	public static boolean Functional_dep_checker(ArrayList<String> temp2) throws SQLException {
		Connection c = null;

		c = DriverManager.getConnection("jdbc:postgresql://localhost:"+index_q3.port+"/"+index_q3.dbName, "postgres", index_q3.password);

		PreparedStatement preparedStatementSelect = null;
		String query = "SELECT * FROM "+index_q3.table;

		preparedStatementSelect = c.prepareStatement(query);
		ResultSet rs = preparedStatementSelect.executeQuery();

		HashMap<String, String> book = new HashMap<>();
		boolean Find = true;
		String RHS = "";
		String LHS = "";
		while (rs.next()) {

			RHS = rs.getString(temp2.get(temp2.size() - 1));
			
			if (RHS == null) {
				// Discard row
				continue;
			}
			RHS.toLowerCase();
			LHS = "";
			for (int i = 0; i < temp2.size() - 1; i++) {

				if (temp2.get(i).equals("movie_id")) {
					String left = Integer.toString(rs.getInt("movie_id")).strip();
					LHS = LHS + left;

				} else {
					String left = rs.getString(temp2.get(i));

					LHS = LHS + left;
				}

			}

			if (!book.containsKey(LHS)) {
				book.put(LHS, RHS);
			} else {
				
				String check = book.get(LHS);
//				if(temp2.get(temp2.size() - 1).equals("genre")) {
//					System.out.println("This is LHS "+LHS);
//					System.out.println("THis is current "+ RHS);
//					System.out.println("This is book "+check);
//				}
					if (!check.equals(RHS)) {
//						if(temp2.get(temp2.size() - 1).equals("genre")) {
//							System.out.println("This is LHS "+LHS);
//							System.out.println("THis is current "+ RHS);
//							System.out.println("This is book "+check);
//						}
					Find = false;
					break;
				}
				
			}
		}

		if (Find) {
			// System.out.println(LHS+" ---> "+RHS);
			return true;
		} else {
			// System.out.println(LHS+" is not FD --> "+RHS);
			return false;
		}

	}

	public static void main(String args[]) throws SQLException {

		Connection c = null;

		c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/prac", "postgres", "");

		PreparedStatement preparedStatementSelect = null;
		String query = "SELECT * FROM newdb";

		preparedStatementSelect = c.prepareStatement(query);

		ResultSet rs = preparedStatementSelect.executeQuery();


		String store[] = new String[10];
		String prac[] = { "A", "B", "C", "D" };
		store[0] = "Movie_id";
		store[1] = "person_id";
		store[2] = "Movie_type";
		store[3] = "Movie_startyear";
		store[4] = "movie_runtime";
		store[5] = "movie_avgrating";
		store[6] = "person_birthyear";
		store[7] = "role";
		store[8] = "genre_id";
		store[9] = "genre";

		Generate_combinations(store);

	

	}

}
