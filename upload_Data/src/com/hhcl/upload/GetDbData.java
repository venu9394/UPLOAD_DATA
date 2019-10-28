package com.hhcl.upload;


import java.beans.Statement;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GetDbData {
	
java.sql.Statement stmt=null;
PreparedStatement ps=null;


public synchronized ResultSet FetchData(String qry, String KeyColumn,ResultSet rs,Connection con) throws SQLException{
	    stmt = con.createStatement();
	    rs = stmt.executeQuery(qry);
		return  rs;	
 }

public synchronized ResultSet FetchData_Emp_DOB(String qry, String KeyColumn,ResultSet rs,Connection con) throws SQLException{
    stmt = con.createStatement();
    rs = stmt.executeQuery(qry);
	return  rs;	
}
public synchronized ResultSet FetchData_Emp_DOJ(String qry, String KeyColumn,ResultSet rs,Connection con) throws SQLException{
    stmt = con.createStatement();
    rs = stmt.executeQuery(qry);
	return  rs;	
}
public synchronized ResultSet FetchDataPrepare(PreparedStatement ps, String KeyColumn,ResultSet rs,Connection con) throws SQLException{
    rs = ps.executeQuery();
	return  rs;	
}

public synchronized ResultSet FetchDataPrepare_2(PreparedStatement ps, String KeyColumn,ResultSet rs,Connection con) throws SQLException{
    rs = ps.executeQuery();
	return  rs;	
}

/*public ArrayList GetDetaFrmDb1(String qry, String KeyColumn) throws SQLException{  // Start Map As Return Type
	
	ArrayList   FetchData = new ArrayList();
	 Connection con=null;
	 PreparedStatement ps=null;
	 ResultSet rs=null;
     java.sql.ResultSetMetaData rsmd=null;
     try{
    Class.forName("com.mysql.jdbc.Driver");
    System.out.println("Driver loaded....!");
    con=DriverManager.getConnection("jdbc:mysql://192.168.30.105:3306/","hcluser","hcluser!23");
    System.out.println("Connection Established....!");
	}catch(Exception E){
	System.out.println("Error At Loading Driver:"+E);
	}

	try{
          ps=con.prepareStatement(qry);
          rs=ps.executeQuery();
          rsmd = rs.getMetaData();
         int columnsNumber = rsmd.getColumnCount();
        // String name = rsmd.getColumnName(i);
        // rsmd.getColumnLabel();
         
        while(rs.next()){
        	
        	ArrayList li=new ArrayList();
        
        	for(int i=1;i<=columnsNumber;i++){
        		String name = rsmd.getColumnName(i);
       	 		//rs.getString(name);
       	 		
       	 	    li.add( rs.getString(name));
       	 	    
       	 		
        	}
        	//System.out.println("To String::"+li.toString());
        	FetchData.add(li);
        	//li.clear();
        	System.out.println("To String Claer::"+FetchData.toString());
        	//System.out.println("To String Claer::"+li.toString());
        }
      
       // System.out.println("Dispatcher");
        
  }
 catch (Exception e2)
   {
	 e2.printStackTrace();
   }finally{
	   con.close();
	   rs.close();
	   ps.close();

   }

 return FetchData;
 }  //close Map Function
*/
/*public Map GetDetaFrmDbMulti(String qry, String KeyColumn,String AppendColumn,String Appender) throws SQLException{  // Start Map As Return Type Multi
	Map   FetchData = new HashMap();
	 Connection con=null;
	 PreparedStatement ps=null;
	 ResultSet rs=null;
     java.sql.ResultSetMetaData rsmd=null;
     try{
    Class.forName("com.mysql.jdbc.Driver");
    System.out.println("Driver loaded....!");
    con=DriverManager.getConnection("jdbc:mysql://192.168.30.105:3306/","hcluser","hcluser!23");
    System.out.println("Connection Established....!");
	}catch(Exception E){
	System.out.println("Error At Loading Driver:"+E);
	}

	try{
          ps=con.prepareStatement(qry);
          rs=ps.executeQuery();
          rsmd = rs.getMetaData();
         int columnsNumber = rsmd.getColumnCount();
        while(rs.next()){
        
        	for(int i=0;i<=columnsNumber;i++){
        		String name = rsmd.getColumnName(i);
       	 		rs.getString("+name+");
       	 		
       	 		FetchData.put(rs.getString(KeyColumn)+""+Appender+""+rs.getString(AppendColumn) ,  rs.getString(name));
        	}
        }
      
        System.out.println("Dispatcher");
        
  }
 catch (Exception e2)
   {
	 e2.printStackTrace();
   }finally{
	   con.close();
	   rs.close();
	   ps.close();

   }

 return FetchData;
 }  //close Map Function
*//*public Map GetDetaFrmDb(String qry, String KeyColumn) throws SQLException{  // Start Map As Return Type
	Map   FetchData = new HashMap();
	
	 Connection con=null;
	 PreparedStatement ps=null;
	 ResultSet rs=null;
     java.sql.ResultSetMetaData rsmd=null;
     try{
    Class.forName("com.mysql.jdbc.Driver");
    System.out.println("Driver loaded....!");
    con=DriverManager.getConnection("jdbc:mysql://192.168.30.105:3306/","hcluser","hcluser!23");
    System.out.println("Connection Established....!");
	}catch(Exception E){
	System.out.println("Error At Loading Driver:"+E);
	}

	try{
          ps=con.prepareStatement(qry);
          rs=ps.executeQuery();
          rsmd = rs.getMetaData();
         int columnsNumber = rsmd.getColumnCount();
        // String name = rsmd.getColumnName(i);
        // rsmd.getColumnLabel();
         
        while(rs.next()){
        	
        	ArrayList li=new ArrayList();
        
        	for(int i=1;i<=columnsNumber;i++){
        		String name = rsmd.getColumnName(i);
       	 		//rs.getString(name);
       	 		
       	 	    li.add( rs.getString(name));
       	 	    
       	 		
        	}
        	//System.out.println("To String::"+li.toString());
        	FetchData.put(rs.getString(KeyColumn), li.toString());
        	li.clear();
        	System.out.println("To String Claer::"+li.toString());
        	//System.out.println("To String Claer::"+li.toString());
        }
      
       // System.out.println("Dispatcher");
        
  }
 catch (Exception e2)
   {
	 e2.printStackTrace();
   }finally{
	   con.close();
	   rs.close();
	   ps.close();

   }

 return FetchData;
 }  //close Map Function


public Map getBasic(String string, String string2, ResultSet rs) {
	// TODO Auto-generated method stub
	return null;
}
*/

public ResultSet fupaid(String qry, String KeyColumn,ResultSet rs,Connection con) {
	    try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    try {
			rs = stmt.executeQuery(qry);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return  rs;	
}

 //close Map Function

}  // Close Main Class



