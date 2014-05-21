package nlsde.buaa.estimate.down;


public class ServiceFatory {

	public static IPatternService getPatternService(){
		return new Pattern2DB();
	}
}
