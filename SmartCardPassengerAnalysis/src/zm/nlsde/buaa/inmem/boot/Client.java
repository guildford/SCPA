package zm.nlsde.buaa.inmem.boot;

import zm.nlsde.buaa.inmem.ml.ODExtraction;
import zm.nlsde.buaa.inmem.ml.PassCount;
import zm.nlsde.buaa.inmem.ml.RegionDivide;
import zm.nlsde.buaa.inmem.model.DataPool;

public class Client {
	
	public static void main(String[] args) throws InterruptedException {
		// load all RDDs
		DataPool.getInstance().loadAll();
		
		// for timing
		double time;
		
		// start threads
		// TODO timer
		Thread odeThread = new Thread(new ODExtraction());
		time = System.currentTimeMillis();
		odeThread.start();
		odeThread.join();
		System.out.println("OD Extration took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		Thread pcThread = new Thread(new PassCount());
		time = System.currentTimeMillis();
		pcThread.start();
		pcThread.join();
		System.out.println("Passenger Count took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		Thread rcThread = new Thread(new RegionDivide());
		time = System.currentTimeMillis();
		rcThread.start();
		rcThread.join();
		System.out.println("Region Divide took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		System.out.println("All Threads Finished.");
	}
}
