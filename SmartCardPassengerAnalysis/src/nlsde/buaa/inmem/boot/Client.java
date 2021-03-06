package nlsde.buaa.inmem.boot;

import nlsde.buaa.inmem.ml.FastPassengerCount;
import nlsde.buaa.inmem.ml.FastRegionChart;
import nlsde.buaa.inmem.ml.FastRegionDivide;
import nlsde.buaa.inmem.ml.ODExtraction;
import nlsde.buaa.inmem.model.DataPool;
import nlsde.buaa.region.RegionDivide;
import nlsde.buaa.stationpassenger.PassengerCount;

public class Client {
	
	public static void main(String[] args) throws InterruptedException {
		// load all RDDs
		DataPool.getInstance().loadAll();
		
		// for timing
		double time;
		
		// start threads
		// TODO timer
		Thread ode = new Thread(new ODExtraction());
		time = System.currentTimeMillis();
		ode.start();
		ode.join();
		System.out.println("OD Extration took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		PassengerCount pc = new PassengerCount();
		
		Thread fpc = new Thread(new FastPassengerCount(pc));
		time = System.currentTimeMillis();
		fpc.start();
		fpc.join();
		System.out.println("Passenger Count took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		System.out.println(pc.result.size());
		pc.outTmpFile();
		pc.outHeatFile();
		
		RegionDivide rd = new RegionDivide();
		
		Thread frd = new Thread(new FastRegionDivide(rd));
		time = System.currentTimeMillis();
		frd.start();
		frd.join();
		System.out.println("Region Divide took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		rd.outTmpFile();
		rd.outAreaFile();
		
		Thread frc = new Thread(new FastRegionChart(rd));
		time = System.currentTimeMillis();
		frc.start();
		frc.join();
		System.out.println("Region Chart took " + (System.currentTimeMillis() - time) / 1000 + " s.");
		
		rd.outChartFile();
		
		System.out.println("All Threads Finished.");
	}
}
