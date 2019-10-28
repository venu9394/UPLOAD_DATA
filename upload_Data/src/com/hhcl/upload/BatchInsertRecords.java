package com.hhcl.upload;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.mysql.jdbc.Connection;

@SuppressWarnings("unused")
public class BatchInsertRecords {
	public  static Map  BatchInsert(String Query,ArrayList DataObject,Connection conn) {
		System.out.println("Query::"+Query);
		@SuppressWarnings("rawtypes")
		Map ReturnMap=new HashMap();
		try{
			conn.setAutoCommit(false);
			PreparedStatement preparedStatement = conn.prepareStatement(Query);
			java.util.Iterator Data1 = DataObject.iterator();
			while(Data1.hasNext()){
				ArrayList list3 = new ArrayList();
				list3=(ArrayList)Data1.next();
				java.util.Iterator Data = list3.iterator();
				while(Data.hasNext()){
					int cnt=0;
					String DataList=Data.next().toString();
					String DataTemp[]=DataList.split(",");
					if(DataTemp[2].equalsIgnoreCase("S")){
						preparedStatement.setString(Integer.parseInt(DataTemp[0].toString()),DataTemp[1]);
						cnt=cnt+1;
					}if(DataTemp[2].equalsIgnoreCase("I")){
						preparedStatement.setInt(Integer.parseInt(DataTemp[0].toString()),Integer.parseInt(DataTemp[1].toString()));
						cnt=cnt+1;
					}if(DataTemp[2].equalsIgnoreCase("D")){
						preparedStatement.setDate(Integer.parseInt(DataTemp[0].toString()),getCurrentDate());
						cnt=cnt+1;
					}if(DataTemp[2].equalsIgnoreCase("T")){
						preparedStatement.setTimestamp(Integer.parseInt(DataTemp[0].toString()),getCurrentTimeStamp());
						cnt=cnt+1;
					}
				}  // while Close
				preparedStatement.addBatch();
			} 
			try {
				int[] rows=preparedStatement.executeBatch();
				ReturnMap.put("InserRows",rows.length);
			} catch (BatchUpdateException b) {
				System.out.println("cvxcvxvc" +b);
				System.err.println("SQLException: " + b.getMessage());
				System.err.println("SQLState: " + b.getSQLState());
				System.err.println("Message: " + b.getMessage());
				System.err.println("Vendor error code: " + b.getErrorCode());
			}
			java.sql.ResultSet res=null;
			res = preparedStatement.getGeneratedKeys(); 
			if (res.next()){ 
				ReturnMap.put("GeneratedKey",(long)res.getInt(1));
			}
			conn.commit();
			preparedStatement.close();
		}catch(Exception er){
			System.out.println("Error at::"+er);
		}
		return ReturnMap;
	}
	private static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}
	private static java.sql.Date getCurrentDate() {
		java.util.Date today = new java.util.Date();
		return new java.sql.Date(today.getTime());
	}
}
