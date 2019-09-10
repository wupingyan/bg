package demo.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestMain {

	public static void main(String[] args) {
		String sql = "select * from mytest1";

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		try{
			//获取链接
			conn = JDBCUtils.getConnection();
			
			//得到运行环境
			st = conn.createStatement();
			
			//运行SQL
			rs = st.executeQuery(sql);
			//处理
			while(rs.next()){
				//取员工姓名, 不能通过列名来取，通过序号
				int id = rs.getInt("tid");
				String name = rs.getString("tname");
				System.out.println(id+"\t"+name);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			//释放资源
			JDBCUtils.release(conn, st, rs);
		}
	}

}
