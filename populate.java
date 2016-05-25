/*************************************************************************/
/*Program     : populate.java                                            */
/*Date        : 2/20/2014                                                */
/*Author      : Sreenidhi Krishna                                        */
/*Description : This program loads the Oracle JDBC driver and connects   */
/*	        through driver.It populates the database with data from  */
/*              the files                                                */ 
/*************************************************************************/
import java.io.BufferedReader;  
import java.io.FileReader;  
import java.util.StringTokenizer;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;

public class populate
{
    //Create globally inside the class
    public Connection conn;
    public Statement st;
	public void connectDB()
	{
	    try{
            // Load the Oracle JDBC driver
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
           
            // Connect through driver using user name and password
            Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
         
            // System.out.println( "Connection established");
            Statement st = conn.createStatement();
   		   } catch (Exception e){
     		 System.out.println( "Error while connecting to DB: "+ e.toString() );
     		 e.printStackTrace();
     		   }
	}

	//Function to read the data files
        //name of files taken as command line arguments
	public void datainsertion(String s1, String s2, String s3) throws SQLException
	{
             
               Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
               //Create a statement object
			   Statement st = conn.createStatement();
			   
			   //Remove any previous data
               st.executeUpdate("delete from hydrant");
               st.executeUpdate("delete from building");
               
               //Initialising Variables
               String line, key, d = ",", line1, key1, d1 = ",";
               BufferedReader text = null, text1 = null;  
               StringTokenizer tk,tk1; 
               String sample="",sample1="";
               int count=0,count1=0;
               int numcoordinates=0,numcoordinates1;
               
               //Read from file
               try{
                    String data1="./"+s2;  //Get the data file
                    text1 = new BufferedReader(new FileReader(data1));  
                    line1 = text1.readLine(); // This gives first line in file  
                    while (line1 != null)  //This keeps reading till the end of file
                    { 
                      tk1 = new StringTokenizer(line1, d1);  
                      System.out.println("Data taken from file: \t" + line1);  
                      System.out.print("Data inserted into database: \t");  
                      count1=0;
                      sample1="";
                      while (tk1.hasMoreTokens())  //This keeps reading till all values-tokens are read
                      {
                        key1 = tk1.nextToken(); 
                        count1++;
                        if(count1==1)
                            sample1+="INSERT INTO hydrant values("+"'"+key1+"'"+",";
                        if(count1==2)
                            sample1+="SDO_Geometry (2001,null,SDO_POINT_TYPE("+key1+",";         
                        if(count1==3)
                            sample1+=key1+",null),null,null))";
                             
                      }
                      System.out.println(sample1);
                      st.executeUpdate(sample1);
                      line1 = text1.readLine();   
                    }

                  }catch (IOException e) {  
                                        e.printStackTrace();
                                         } finally {
                                                   try {
                                                        if (text1 != null)text1.close();
                                                       } catch (IOException ex){
                                                         ex.printStackTrace(); }
              //Read data from file                                      }
              String sample2="";
                try{
            	   String data3 ="./"+s1;   //Get the data file
            	   text = new BufferedReader(new FileReader(data3));  
                   line = text.readLine();  // This gives first line in file  
                   while (line != null)  //This keeps reading till all values-tokens are read
                   { 
                     tk = new StringTokenizer(line, d);  
                     System.out.println("Data taken from file:  \t" + line);  
                     System.out.print("Data inserted into database:\t");  
                     count=0;
                     sample="";
                     while (tk.hasMoreTokens())  
                     {
                       key = tk.nextToken(); 
                       count++;
                       if(count==1)
                           sample+="INSERT INTO building values("+"'"+key+"'"+",";
                       if(count==2)
                           sample+="'"+key+"'"+",";
                       if(count==3){
                           sample+="SDO_Geometry (2003,null,null,SDO_ELEM_INFO_ARRAY(1,1003,1),SDO_ORDINATE_ARRAY(";
                           key=key.trim();
                           numcoordinates=Integer.parseInt(key);
                           System.out.println("cord: "+numcoordinates);
                                   }        
                       if(count>3 && count<((2*numcoordinates)+3))
                           { 
                    	   sample+=key+",";
                                                 
                           }
                       if(count==((2*numcoordinates)+3))
                           {   sample+=key;
                          
                           }
                     }
                     sample+=")),'n')"; //Closing the statement
                     System.out.println(sample);
                     st.executeUpdate(sample);
                     line = text.readLine(); // next line  
                   }
                  }catch (IOException e) {
                                       e.printStackTrace();
                                         } finally {
                                                    try {
                                                         if (text != null)text.close();
                                                        } catch (IOException ex) {
                                                           ex.printStackTrace(); }
                                                   }
                 try{
                    String data2 = "./"+s3;  //Get the data file
                    text = new BufferedReader(new FileReader(data2));  
                    line = text.readLine(); // This gives first line in file  
                    while (line != null)  //This keeps reading till all values-tokens are read
                    { 
                      tk = new StringTokenizer(line);  
                      System.out.println("Data taken from file: \t" + line);  
                      System.out.print("Data inserted into database: \t");  
                      while (tk.hasMoreTokens())  
                      {
                         key = tk.nextToken(); 
                         System.out.println(key);
                         sample2="UPDATE building SET building_on_fire='y' where building_name=' "+key+"'";
                         st.executeUpdate(sample2);
                         System.out.println(sample2);
                      }
					 line = text.readLine(); // next line  
                    }
                  }catch (IOException e) {
                                          e.printStackTrace();
                                         } finally {
                                                    try {
                                                          if (text != null)text.close();
                                                        } catch (IOException ex) {
                                                           ex.printStackTrace(); }
                                                   }
               
          try {
                conn.commit();
                System.out.println("Data inserted successfully");
                conn.close();
                System.out.println("Connection closed");
              } catch (SQLException ex){
                System.out.println("error in closing connection"); 
               
                }finally{ }
                }
 
    }
   
        
    public static void main(String[] args)
	{
        //Main function- Calls connect method and populates tables

        populate popl = new populate();
		popl.connectDB(); // Make connection to database
                try {
                    //Call method to populate the database
                    popl.datainsertion(args[0],args[1],args[2]);
                    } catch (SQLException ex) {
			System.out.println("error in opening input file");
                                        }
        
    }
}
