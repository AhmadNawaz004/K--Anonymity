package merapehlaproject;
import java.awt.List;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.util.HashSet;

import com.mysql.cj.protocol.Resultset;

public class Kanonymity {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:/test3";
	
	static final String USER = "root";
	static final String PASS = "";
	static ResultSet resultset = null;
	static ResultSet resultsett = null;
	static ResultSet resultset2 = null;
	static ResultSet resultset3 = null;
	static ResultSet resultsettemp = null;
	static ResultSetMetaData rsmd = null;
	static PreparedStatement preparedstmt = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Connection conn = null;
		Statement stmt = null;
		Statement stmtt = null;
		Statement stmttt = null;
		int number_of_groups=0;
		Date date = new Date();
		String starting_time = date.toString();

		long startTime = System.nanoTime();
		
			try {
				conn = DriverManager.getConnection(DB_URL,USER,PASS);
				//int i=1;
				//F-1 Code for making TPDT
	
				stmt = conn.createStatement();
				stmtt = conn.createStatement();
				stmttt = conn.createStatement();
				
					
				//------Making	TPDT' ----------------\\				
				
				resultset = (ResultSet) stmt.executeQuery("Select * from transformed2");
				
				int row=1; int gidd=1; int counter=0;
				 
				while(resultset.next())
				{
					System.out.println(gidd);
					
					if(resultset.getInt(8)==0)
					{
						String insertion2 = "update transformed2 set gid=? where duid ='" + resultset.getString(1) + "'";
						PreparedStatement preparedstmt = conn.prepareStatement(insertion2);
						preparedstmt.setString(1, Integer.toString(gidd));
						preparedstmt.execute();		//******************
					}
					
					counter=counter+1;
					
					if(counter % 5 == 0)		//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX1
					{
						
						gidd=gidd+1;
						counter=0;
					}
					
					
					//System.out.print(resultset.getString(3));
					//System.out.println();
				}
				resultset.close();
				
				
				//-----------------Anonymization--------------
				
				int num_of_groups=gidd; int group_id=1;
				
				while(group_id <= num_of_groups)
				{
					resultset = (ResultSet) stmt.executeQuery("Select min(cast(DOBMM AS SIGNED)), max(cast(DOBMM AS SIGNED)), min(cast(DOBYY AS SIGNED)), max(cast(DOBYY AS SIGNED)), min(cast(EDUY AS SIGNED)), max(cast(EDUY AS SIGNED)), min(cast(INCOME AS SIGNED)), max(cast(INCOME AS SIGNED) ) from transformed2 where gid = '" + group_id +"'");
					
					while(resultset.next())
					{			
						System.out.println("gg");
						String colvalue1,colvalue2,colvalue3,colvalue4,colvalue5,colvalue6,colvalue7,colvalue8 = "0";
						
						colvalue1 = resultset.getString(1); System.out.print(" Min DOBMM : " + colvalue1 + " ");
						colvalue2 = resultset.getString(2); System.out.print(" Max DOBMM : " + colvalue2 + " ");
						colvalue3 = resultset.getString(3); System.out.print(" Min DOBYY : " + colvalue3 + " ");
						colvalue4 = resultset.getString(4); System.out.print(" Max DOBYY : " + colvalue4 + " ");
						colvalue5 = resultset.getString(5); System.out.print(" Min EDUY : " + colvalue5 + " ");
						colvalue6 = resultset.getString(6); System.out.print(" Max EDUY : " + colvalue6 + " ");
						colvalue7 = resultset.getString(7); System.out.print(" Min INCOME : " + colvalue5 + " ");
						colvalue8 = resultset.getString(8); System.out.print(" Max INCOME : " + colvalue6 + " ");
						
						
						String insertion2 = "update transformed2 set DOBMM=?, DOBYY=?, EDUY=?, INCOME=? where gid ='" + group_id + "'";
						PreparedStatement preparedstmt = conn.prepareStatement(insertion2);
						preparedstmt.setString(1, colvalue1+"_"+colvalue2);
						preparedstmt.setString(2, colvalue3+"_"+colvalue4);
						preparedstmt.setString(3, colvalue5+"_"+colvalue6);
						preparedstmt.setString(4, colvalue7+"_"+colvalue8);
						
						preparedstmt.execute();
						
						
//						System.out.println("Handling Row # " + row_number);
//						
//						row_number = row_number + 1;
					}
					
					//System.out.println("----------------------------------------------");
					
					group_id = group_id + 1;
					
				}//while loop brace
				
				//-----------------------Anonymization Done------------------------------
				
				//Extending Groups
				
//				resultset = (ResultSet) stmt.executeQuery("Select * from transformed where DUID=20017");
//				
//				while(resultset.next())
//				{
//					resultset2 = (ResultSet) stmtt.executeQuery("Select * from transformed2 where RIGHT(DOBMM, InStr(DOBMM,'_'))< '" + resultset.getInt(2) + "'");
//					//resultset2 = (ResultSet) stmtt.executeQuery("Select RIGHT(DOBMM, inStr(DOBMM,'_')) from transformed2");
//					//resultset2 = (ResultSet) stmtt.executeQuery("Select * from transformed2 where RIGHT(DOBMM, inStr(DOBMM,'_') < '" + resultset.getString(2) + "'" );
//					while(resultset2.next())
//					{
//						System.out.println("DOBMM = " + resultset2.getString(2) + " GID = " + resultset2.getString(8));
//						//System.out.println("DOBMM = " + resultset2.getString(1));
//					}
//				}
				
				//Extending Groups Done
				
					//--------Everything Done---------------\\
					
				
				
					
					//-------------------------------------------------\\
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//F-1 -------------
		System.out.println("Hello");
	}

}
