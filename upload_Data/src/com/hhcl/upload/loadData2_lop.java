package com.hhcl.upload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.mysql.jdbc.Statement;
public class loadData2_lop {
public static void main(String args[]){
Connection con1=null;
Connection con2=null;
Statement statement=null;
ResultSet Res=null;
GetDbData DataObj=new GetDbData();
StringBuffer empSequenceList=new StringBuffer();
System.out.println("HI....!");
Map primary_data=new HashMap();
ArrayList primaryList=new ArrayList();

try
{
  con1 = com.util.local.Util.getConnection();
  con1.setAutoCommit(false);
  System.out.println("Connection Established....!");
} catch (Exception e) {
  e.printStackTrace();
}
//********** Primary Data ********************************
StringBuffer data_primary_fetch=new StringBuffer();
//data_primary_fetch.append(" SELECT empid,f1,f2,f3,f4 FROM testing.new_table_field where status in (1008)");

data_primary_fetch.append(" SELECT empid,f1 f1,f2,f3,f4 FROM testing.new_table_field where status in (1001) and f1>0 ");

Res=null;
try {
	Res=(ResultSet)DataObj.FetchData(data_primary_fetch.toString(), "PayrollStatus", Res ,con1);
	// rsmd =(ResultSetMetaData)Res.getMetaData();
	// String name = rsmd.getColumnName(1);
	//int columnCount = rsmd.getColumnCount();
	//BusinesUnit.add("Select#--Select One--");
	while(Res.next()){
		primaryList.add(Res.getString("empid"));
		empSequenceList.append(Res.getString("empid"));
		empSequenceList.append(",");
		primary_data.put(Res.getString("empid")+"_Seqnum", Res.getString("empid"));
		primary_data.put(Res.getString("empid")+"_f1", Res.getString("f1"));
		primary_data.put(Res.getString("empid")+"_f2", Res.getString("f2"));
		primary_data.put(Res.getString("empid")+"_f3", Res.getString("f3"));
		primary_data.put(Res.getString("empid")+"_f4", Res.getString("f4"));
	}
	empSequenceList.append("00000000");
}catch(Exception Er2){
	 System.out.println("Exception:: "+Er2);
}finally{
   if(con1!=null){
	   try {
		con1.close();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
}

System.out.println("empSequenceList::" +empSequenceList.toString());
//********** Primary Data ********************************
statement=null;
Res=null;
try
{
  con2 = com.util.prod.Util.getConnection();
  con2.setAutoCommit(false);
  System.out.println("Connection Established....!");
} catch (Exception e) {
  e.printStackTrace();
}
//********** Primary Data ********************************
StringBuffer data_primary_fetch1=new StringBuffer();
data_primary_fetch1.append(" SELECT employeesequenceno,employeeid,companyid,status from hclhrm_prod.tbl_employee_primary where employeesequenceno in("+empSequenceList.toString()+") ");
Res=null;
try {
	Res=(ResultSet)DataObj.FetchData(data_primary_fetch1.toString(), "PayrollStatus", Res ,con2);
	
	System.out.println("data_primary_fetch1.toString()::" +data_primary_fetch1.toString());
	// rsmd =(ResultSetMetaData)Res.getMetaData();
	// String name = rsmd.getColumnName(1);
	//int columnCount = rsmd.getColumnCount();
	//BusinesUnit.add("Select#--Select One--");
	while(Res.next()){
		//primaryList.add(Res.getString("empid"));
		primary_data.put(Res.getString("employeesequenceno")+"_empid", Res.getString("employeeid"));
		primary_data.put(Res.getString("employeesequenceno")+"_companyid", Res.getString("companyid"));
		
		primary_data.put(Res.getString("employeesequenceno")+"_status", Res.getString("status"));
		
	}
}catch(Exception Er2){
	 System.out.println("Exception:: "+Er2);
}/*finally{
   if(con2!=null){
	   try {
		con2.close();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
}*/
System.out.println("primary_data" +primary_data.toString());
System.out.println("primaryList" +primaryList.toString());
Iterator empseq=primaryList.iterator();


try{
	  
	  
	  StringBuffer UpdateITS=new StringBuffer();
	  
	/*  UpdateITS.append("update hclhrm_prod.tbl_employee_personalinfo set PAN=? ");
	  UpdateITS.append(" where employeeid=? ");*/
	  
	   
	 /* UpdateITS.append(" insert into hclhrm_prod.tbl_employee_tds ");
	  UpdateITS.append("  (TRANSACTIONID, BUSINESSUNITID, EMPLOYEEID, TDSVALUE, STATUS, EMPLOYEESTATUS) ");
	  UpdateITS.append("  values(201812,?,?,?,'D',1001)  ");
	  */
	  
	  UpdateITS.append(" insert into hclhrm_prod.tbl_employee_lop(LOPTRANSACTIONID, BUSINESSUNITID, EMPLOYEEID, LOPCOUNT, STATUS, EMPLOYEESTATUS) ");
	  UpdateITS.append(" values(201812,?,?,?,'C',1001) ");
	 
	 /* UpdateITS.append(" insert into test_mum.tbl_emp_tds_extra_80d_cal(EMPLOYEEID, S80D, D80D, CTC,LUPDATE, STATUS, FY) ");
	  UpdateITS.append(" values(?,?,?,?,now(),1001,'2018-2019' ) ");
	  String Mode="80D";*/
	
	  String Mode="LOP";
	  
	  int[] l={};    	      
	  PreparedStatement ps = con2.prepareStatement(UpdateITS.toString());
	  
	  while(empseq.hasNext()){
			String emp_seq=empseq.next().toString();
			
			System.out.println("emp_seq::" +emp_seq);
			String f1=primary_data.get(emp_seq+"_f1").toString();
			String f2=primary_data.get(emp_seq+"_f2").toString();
			String f3=primary_data.get(emp_seq+"_f3").toString();
			String f4=primary_data.get(emp_seq+"_f4").toString();
			
			int EMPID=Integer.parseInt(primary_data.get(emp_seq+"_empid").toString());
			String status=primary_data.get(emp_seq+"_status").toString();
			//String EMPID=primary_data.get(emp_seq+"_empid").toString();
			
			String companyid=primary_data.get(emp_seq+"_companyid").toString();
			//***********PAN**************
			/*ps.setInt(1, EMPID);
			 ps.setString(2, PAN_Insert);*/
			
			/*ps.setString(1, PAN_Insert);
			 ps.setInt(2, EMPID);*/
			//**********PAN END **********
			if( Mode.equalsIgnoreCase("TDS")){	
				
				//************TDS & LOP**************
				  ps.setString(1, companyid);
				  ps.setInt(2, EMPID);
				  ps.setString(3, f1);
				  ps.addBatch();
				//*************TDS END & LOP****************
			}else if(Mode.equalsIgnoreCase("LOP")){
				
				//************TDS & LOP**************
				  ps.setString(1, companyid);
				  ps.setInt(2, EMPID);
				  ps.setString(3, f1);
				  ps.addBatch();
				//*************TDS END & LOP****************
			
			}else if(status.equalsIgnoreCase("1001")  && !Mode.equalsIgnoreCase("80D") && !Mode.equalsIgnoreCase("TDS")){
			//************TDS & LOP**************
			  ps.setString(1, companyid);
			  ps.setInt(2, EMPID);
			  ps.setString(3, f1);
			  ps.addBatch();
			//*************TDS END & LOP****************
			
			ps.addBatch();
			}else if(Mode.equalsIgnoreCase("80D")){
				
				ps.setString(1, emp_seq);
				ps.setString(2, f1);
				ps.setString(3, f2);
				ps.setString(4, f3);
				
				ps.addBatch();
				
			}
			
			
	  }
	  System.out.println(ps.toString());
	 l=ps.executeBatch();
	  System.out.println("Record Length:: "+l.length);
	 con2.commit();
	}catch(Exception err){
		
		err.printStackTrace();
	}

/*while(empseq.hasNext()){
	
	String emp_seq=empseq.next().toString();
	
	System.out.println(primary_data.get(emp_seq+"__Seqnum") +"~ Employeesequenceno ~"+  emp_seq);
	System.out.println(primary_data.get(emp_seq+"_empid") +"~ empid ~"+  emp_seq);
	
	System.out.println(primary_data.get(emp_seq+"_f1") +"~ f1 ~"+  emp_seq);
	System.out.println(primary_data.get(emp_seq+"_f2") +"~ f2 ~"+  emp_seq);
	System.out.println(primary_data.get(emp_seq+"_f3") +"~ f3 ~"+  emp_seq);
	System.out.println(primary_data.get(emp_seq+"_companyid") +" ~companyid ~"+  emp_seq);
	
	
	
}*/







}

}
