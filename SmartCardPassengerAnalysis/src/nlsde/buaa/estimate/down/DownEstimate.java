package nlsde.buaa.estimate.down;

import java.util.List;

public class DownEstimate {

	private IPatternService ips; 
	
	public DownEstimate(){
		ips = ServiceFatory.getPatternService();
	}
	
	public void estimate(){
		CardBean card=null;//TODO:read in card
		this.estimate(card);
		//TODO:save card
	}
	
	public void estimate(CardBean card){
		PassengerPattern pp=ips.getPattern(card.getCardID());
		List<PatternBean> pl=pp.getPatternList();
		PatternBean cp=getCorrectPattern(pl,card);
		setDownInfo(card,cp);
	}
	
	private PatternBean getCorrectPattern(List<PatternBean> pl,CardBean card){
		//TODO: get the most probably pattern
		return null;
	}
	
	private void setDownInfo(CardBean card,PatternBean cp){
		//TODO: get the down station and time through line-schedule
	}
}
