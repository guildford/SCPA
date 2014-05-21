package nlsde.buaa.estimate.down;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import nlsde.buaa.base.configer.DBPool;

public class Pattern2DB implements IPatternService{
	
	Connection conn;

	public Pattern2DB(){
		conn=DBPool.getDBConnection();
	}

	@Override
	public PassengerPattern getPattern(String id) {
		PassengerPattern pp=new PassengerPattern(id);
		try {
			String sql="select pattern from individual_pattern where id = ?";
			PreparedStatement st=conn.prepareStatement(sql);
			st.setString(1, id);
			ResultSet rs=st.executeQuery();
			if (rs.next()){
				pp.setPattern(rs.getString("pattern"));
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return pp;
	}

}
