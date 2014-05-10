package zm.nlsde.buaa.inmem.model;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import zm.nlsde.buaa.inmem.boot.AppConf;
import zm.nlsde.buaa.inmem.boot.Client;

public class DataPool {
	
	private static JavaSparkContext jsc = null;
	
	static {
		// TODO configurable
		// initial jsc
		SparkConf spconf = new SparkConf();
		if (AppConf.isDebug) {
			spconf.setMaster("local[2]");
		} else {
			spconf.setMaster("yarn-standalone");
		}
		spconf.setAppName("SCPA");
		spconf.setSparkHome("/usr/etc/spark-0.9.0");
		spconf.setJars(JavaSparkContext.jarOfClass(Client.class));
		jsc = new JavaSparkContext(spconf);
		
		// TODO broadcast related
		System.setProperty("spark.broadcast.factory", "org.apache.spark.broadcast." + AppConf.BROADCAST_NAME + "BroadcastFactory");
	    System.setProperty("spark.broadcast.blockSize", AppConf.BROADCAST_BLOCKSIZE);
	}
	
	private HashMap<String, JavaRDD<String>> data = new HashMap<String, JavaRDD<String>>();
	// TODO
	private HashMap<String, JavaPairRDD<String, String>> pairdata = new HashMap<String, JavaPairRDD<String, String>>();
	
	private static class DataPoolHolder {
		private static final DataPool INSTANCE = new DataPool();
	}

	private DataPool() {
		
	}
	
	public static final DataPool getInstance() {
		return DataPoolHolder.INSTANCE;
	}
	
	public void loadAll(){
		// map all types of data
		for (String name: AppConf.DATA_TABLE.keySet()) {
			getInstance().put(name, jsc.textFile(AppConf.DATA_TABLE.get(name)));
		}
		
		// cache data into distributed memory
		for (JavaRDD<String> rdd: this.data.values()) {
			rdd.cache().count();
		}
	}

	public Boolean put(String name, JavaRDD<String> data) {
		// if memory enough
		if (!this.isMemoryAdequate()) {
			System.err.println("There is no adequate memory to cache.");
			return false;
		}
		
		// if data already exists
		if (this.data.containsKey(name)) {
			System.err.println("Cannot override data.");
			return false;
		} else {
			this.data.put(name, data);
			return true;
		}
	}
	
	public HashMap<String, JavaRDD<String>> getAll() {
		return this.data;
	}

	public JavaRDD<String> get(String threadname, String datatype) {
		if (this.verifyReadPermission(threadname, datatype)) {
			return this.data.get(datatype);
		}
		return null;
	}
	
	public HashMap<String, JavaRDD<String>> get(String threadname, String[] datatypelist) {
		HashMap<String, JavaRDD<String>> data = new HashMap<String, JavaRDD<String>>();
		for (String datatype : datatypelist) {
			if (this.verifyReadPermission(threadname, datatype) && this.data.containsKey(datatype)) {
				data.put(datatype, this.data.get(datatype));
			}
		}
		return data.size() == 0 ? null : data;
	}
	
	public Boolean verifyReadPermission(String threadname, String datatype) {
		for (String type: AppConf.PERMISSION_TABLE.get(threadname)) {
			if (type.equalsIgnoreCase(datatype)) {
				return true;
			}
		}
		System.err.println(threadname + "is applying illegal data.");
		return false;
	}
	
	// TODO
	public boolean isMemoryAdequate() {
		return true;
	}
	
	public Object broadcast(Object o) {
		return DataPool.jsc.broadcast(o).value();
	}
	
}
