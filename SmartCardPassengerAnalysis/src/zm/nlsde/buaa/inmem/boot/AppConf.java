package zm.nlsde.buaa.inmem.boot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import zm.nlsde.buaa.inmem.ml.ODExtraction;
import zm.nlsde.buaa.inmem.ml.PassCount;
import zm.nlsde.buaa.inmem.ml.RegionDivide;

public class AppConf {
	public static final Boolean isDebug = true;

	public static final String HDFS_ROOT = "hdfs://mycluster/user/zming/";
	
	public static final String BROADCAST_NAME = "Http";
	
	public static final String BROADCAST_BLOCKSIZE = "4096";

	public static final HashMap<String, String> DATA_TABLE = new HashMap<String, String>();
	
	public static final HashMap<String, List<String>> PERMISSION_TABLE = new HashMap<String, List<String>>();
	
	// initiate DATA_TABLE
	static {
		if (AppConf.isDebug) {
			DATA_TABLE.put("SMARTCARD", "/usr/zming/data/card/sample.csv");
			DATA_TABLE.put("STATION_INFO", "baseData/stationinfo.csv");
		} else {
			DATA_TABLE.put("SMARTCARD", AppConf.HDFS_ROOT + "card/BUScard/");
			DATA_TABLE.put("STATION_INFO", AppConf.HDFS_ROOT + "ref/stationinfo.csv");
		}
	}
	
	// register data for each thread
	static {
		PERMISSION_TABLE.put(ODExtraction.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(ODExtraction.class.getName()).add("SMARTCARD");
		PERMISSION_TABLE.get(ODExtraction.class.getName()).add("STATION_INFO");
		
		PERMISSION_TABLE.put(PassCount.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(PassCount.class.getName()).add("SMARTCARD_FULLOD");
		
		PERMISSION_TABLE.put(RegionDivide.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(RegionDivide.class.getName()).add("POINT_COUNT");
	}
	
}
