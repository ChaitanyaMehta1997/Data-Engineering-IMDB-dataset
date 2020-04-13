//package Big_data;

// @author : Chaitanya

//Creates rating and NumVotes hashmaps to enable checks in O(1)

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

public class Rating_Map {
	public static HashMap<Integer, Double> Rating_Map  =new HashMap<>() ;
	public static HashMap<Integer, Double> Number_Map =  new HashMap<>();
	

	@SuppressWarnings("resource")
	public static void main(String args[]) throws FileNotFoundException, IOException {

		BufferedReader br = null;
		//String filename = "C:\\Users\\13155\\Desktop\\Chaitanya\\Sem2\\BigData\\title.ratings.tsv.gz\\";
		String filename = "title.ratings.tsv.gz";

		br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(filename))));
		int count = 0;
		
		//HashMap<Integer, Double> Rating_Map = new HashMap<>();
	//	HashMap<Integer, Double> Number_Map = new HashMap<>();
		int count2=0;
		while (br.ready()) {

			if (count == 0) {
				count += 1;
				System.out.println(br.readLine());
				continue;
				
			}
			String Row = br.readLine();
			String RowStore[] = Row.split("\t");
			String Movie = RowStore[0].substring(2, RowStore[0].length());
			System.out.println(Movie);
			int Movie_primary_key = Integer.parseInt(Movie);
			
			Double Rating=  Double.parseDouble(RowStore[1]);
			Double Number_count =  Double.parseDouble(RowStore[2]);
			
			Rating_Map.put(Movie_primary_key, Rating); 
			Number_Map.put(Movie_primary_key, Number_count);
			//count2+=1;
			//if(count2==1000) {
			//	break;
		//}
			
			
		}
		
          
      
        
	}
}
