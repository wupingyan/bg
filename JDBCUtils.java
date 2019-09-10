package demo.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/*
 * �����ࣺ
 * 1����ȡ���ݿ�Hive����
 * 2���ͷ���Դ
 */
public class JDBCUtils {

	//Hive���ݿ������
	private static String driver = "org.apache.hive.jdbc.HiveDriver";                                
	
	//Hiveλ��
	private static String url = "jdbc:hive2://192.168.157.111:10000/default";

	
	//ע������
	static{
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e);
		}
	}

	//��ȡ����
	public static Connection getConnection(){
		try {
			return DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void release(Connection conn,Statement st,ResultSet rs){
		if(rs != null){
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				rs = null;
			}
		}
		
		if(st != null){
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				st = null;
			}
		}
		
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				conn = null;
			}
		}
	}
}
















