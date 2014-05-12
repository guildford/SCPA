package zm.nlsde.buaa.inmem.model;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
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
		for (JavaRDD<String> rdd: data.values()) {
			rdd.cache().count();
		}
	}

	public Boolean put(String name, JavaRDD<String> data) {
		// memory capacity
		if (!this.isMemoryAdequate()) {
			System.err.println("No adequate memory to cache.");
			return false;
		}
		
		// RDDs are read-only
		if (this.data.containsKey(name)) {
			System.err.println(name + " already exists.");
			return false;
		} else {
			this.data.put(name, data);
			return true;
		}
	}
	
	public Boolean put(String name, JavaPairRDD<String, String> pair) {
		// memory capacity
		if (!this.isMemoryAdequate()) {
			System.err.println("No adequate memory to cache.");
			return false;
		}
		
		// RDDs are read-only
		if (this.pairdata.containsKey(name)) {
			System.err.println(name + " already exists.");
			return false;
		} else {
			this.pairdata.put(name, pair);
			return true;
		}
	}
	
	public HashMap<String, JavaRDD<String>> getAllRDD() {
		return this.data;
	}
	
	public HashMap<String, JavaPairRDD<String, String>> getAllPairRDD() {
		return this.pairdata;
	}

	public JavaRDDLike<? extends Serializable, ?> get(String threadname, String datatype, Boolean isPairRDD) {
		if (this.verifyReadPermission(threadname, datatype)) {
			return (isPairRDD ? this.pairdata : this.data).get(datatype);
		}
		return null;
	}
	
	public JavaRDDLike<? extends Serializable, ?> get(String threadname, String datatype) {
		return this.get(threadname, datatype, false);
	}
	
	public HashMap<String, JavaRDDLike<? extends Serializable, ?>> get(String threadname, String[] datatypelist, Boolean isPairRDD) {
		HashMap<String, JavaRDDLike<? extends Serializable, ?>> result = new HashMap<String, JavaRDDLike<? extends Serializable, ?>>();
		
		for (String datatype : datatypelist) {
			if (this.verifyReadPermission(threadname, datatype) && (isPairRDD ? this.pairdata : this.data).containsKey(datatype)) {
				result.put(datatype, (isPairRDD ? this.pairdata : this.data).get(datatype));
			}
		}
		return result.size() == 0 ? null : result;
	}
	
	public HashMap<String, JavaRDDLike<? extends Serializable, ?>> get(String threadname, String[] datatypelist) {
		return this.get(threadname, datatypelist, false);
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
