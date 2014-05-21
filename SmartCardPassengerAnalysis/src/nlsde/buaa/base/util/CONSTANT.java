package nlsde.buaa.base.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

public class CONSTANT {
	public final static int AFC=1;
	public final static int BUS=2;
	
	public static int CardType=1;

	private static HashMap<String,String> STATION_TO_POSITION=null;
	
	public static HashMap<String,String> get_STATION_TO_POSITION(){
		if (STATION_TO_POSITION==null)
			initial_STATION_TO_POSITION();
		return STATION_TO_POSITION;
	}
	
	private static void initial_STATION_TO_POSITION(){
		STATION_TO_POSITION=new  HashMap<String,String>();
		try {
			File pfile=new File("baseData\\trackstop.csv");
			BufferedReader pin = new BufferedReader(new InputStreamReader(
					new FileInputStream(pfile), "gbk"));
			String line;
			while ((line=pin.readLine())!=null){
				add_STATION_TO_POSITION(line);
			}
			pin.close();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void add_STATION_TO_POSITION(String line){
		String[] data=line.split(",");
		String key=data[0]+","+ data[1];
		String value=data[3]+","+ data[4];
		STATION_TO_POSITION.put(key, value);
	}
}
