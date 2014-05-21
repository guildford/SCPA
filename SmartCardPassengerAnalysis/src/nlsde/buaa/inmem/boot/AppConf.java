package nlsde.buaa.inmem.boot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import nlsde.buaa.inmem.ml.FastPassengerCount;
import nlsde.buaa.inmem.ml.FastRegionChart;
import nlsde.buaa.inmem.ml.FastRegionDivide;
import nlsde.buaa.inmem.ml.ODExtraction;

public class AppConf {
	public static final Boolean isDebug = false;

	public static final String HDFS_ROOT = "hdfs://mycluster/user/zming/";
	
	public static final String BROADCAST_NAME = "Http";
	
	public static final String BROADCAST_BLOCKSIZE = "4096";
	
	public static final int TIME_PRECISION = 10;

	public static final HashMap<String, String> DATA_TABLE = new HashMap<String, String>();
	
	public static final HashMap<String, List<String>> PERMISSION_TABLE = new HashMap<String, List<String>>();
	
	// initiate DATA_TABLE
	static {
		if (AppConf.isDebug) {
			DATA_TABLE.put("SMARTCARD", "/usr/zming/data/card/20120831.csv");
			DATA_TABLE.put("STATION_INFO", "baseData/stationinfo.csv");
		} else {
			DATA_TABLE.put("SMARTCARD", AppConf.HDFS_ROOT + "card/BUScard/20120831.csv");
			DATA_TABLE.put("STATION_INFO", AppConf.HDFS_ROOT + "ref/stationinfo.csv");
		}
	}
	
	// register data for each thread
	static {
		PERMISSION_TABLE.put(ODExtraction.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(ODExtraction.class.getName()).add("SMARTCARD");
		PERMISSION_TABLE.get(ODExtraction.class.getName()).add("STATION_INFO");
		
		PERMISSION_TABLE.put(FastPassengerCount.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(FastPassengerCount.class.getName()).add("SMARTCARD_FULLOD");
		
		PERMISSION_TABLE.put(FastRegionDivide.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(FastRegionDivide.class.getName()).add("POINT_COUNT");
		
		PERMISSION_TABLE.put(FastRegionChart.class.getName(), new ArrayList<String>());
		PERMISSION_TABLE.get(FastRegionChart.class.getName()).add("POINT_COUNT");
	}
	
}
