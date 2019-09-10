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
			//��ȡ����
			conn = JDBCUtils.getConnection();
			
			//�õ����л���
			st = conn.createStatement();
			
			//����SQL
			rs = st.executeQuery(sql);
			//����
			while(rs.next()){
				//ȡԱ������, ����ͨ��������ȡ��ͨ�����
				int id = rs.getInt("tid");
				String name = rs.getString("tname");
				System.out.println(id+"\t"+name);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			//�ͷ���Դ
			JDBCUtils.release(conn, st, rs);
		}
	}

}
