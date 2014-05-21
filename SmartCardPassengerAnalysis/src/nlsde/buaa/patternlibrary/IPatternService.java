package nlsde.buaa.patternlibrary;


public interface IPatternService {

	public PassengerPattern getPattern(String id);
	
	public void savePattern(PassengerPattern pp);
	
	public void updatePattern(PassengerPattern pp);
	
}
