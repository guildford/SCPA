package nlsde.buaa.patternlibrary;


public class ServiceFatory {

	public static IPatternService getPatternService(){
		return new Pattern2DB();
	}
}
