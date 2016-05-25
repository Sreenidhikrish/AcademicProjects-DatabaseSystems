/****************************************************************************/
/*Program     : databasequery.java                                          */
/*Date        : 2/20/2014                                                   */
/*Author      : Sreenidhi Krishna                                           */
/*Description : This program executes queries				    */
/*(whole range, range query, closest hydrant,neighbor buildings) on database*/
/*             								    */   
/****************************************************************************/
import java.io.BufferedReader;  
import java.io.FileReader;  
import java.util.StringTokenizer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class databasequery 
{
    //Creates object of populate class
    populate p= new populate();
    public Connection conn; 
    
    //Function to Get the coordinates of whole region 
    
    public ArrayList<Integer> wholeRegion_vert()
    {
    	
    	     p.connectDB();//Make connection
             ArrayList<Integer> arrayset= new ArrayList<Integer>(); //Create an array list to store the coordinates
            
             try
	     	{
                
			 String sql = "SELECT t.X,t.Y FROM building Tb, TABLE(SDO_UTIL.GETVERTICES(Tb.building_shape)) t";
			 conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
	        
	         Statement s = conn.createStatement();
	         ResultSet rs = s.executeQuery(sql);
		 	 
        	 while(rs.next())
		 	  {
        		         		 
                 arrayset.add(rs.getInt(1));   //Adds t.x
                 arrayset.add(rs.getInt(2));   //Adds t.y
              }
        
		    }
	    	catch(Exception e)
		    {
		    	System.out.println(e.toString());
		    }finally {
		    	if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		             }

          
            return arrayset;
    }
        
    
        
    //Function to Get the coordinates of fire buildings
    
     public ArrayList<Integer> wholeRegion_vertfirebuilding()
     {
    	 p.connectDB();   
    	 ArrayList<Integer> arrayset= new ArrayList<Integer>();

            try
		      {
			   String sql = "SELECT t.X,t.Y FROM building Tb, TABLE(SDO_UTIL.GETVERTICES(Tb.building_shape)) t where building_on_fire='y'";
			 			
			   Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
			   Statement s = conn.createStatement();
 		       ResultSet rs = s.executeQuery(sql);
 	
               while(rs.next())
			     {
                       arrayset.add(rs.getInt(1));   //Adds t.x
                       arrayset.add(rs.getInt(2));   //Adds t.y
                     
                            
                  }
               System.out.println("\nFirebuilding vertices ");
               System.out.println(arrayset);
               
		      }
            catch(Exception e)
		    {
		    	System.out.println(e.toString());
		    }finally {
		    	if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		             }
     return arrayset;
     }
     
  
  //Function to Get the coordinates of hydrants

    public ArrayList<Integer>  wholeRegion_hydrant()
     {
    	p.connectDB();
    	ArrayList<Integer> arrayset= new ArrayList<Integer>();
        try
		{
			String sql = "SELECT t.X,t.Y FROM hydrant Tb, TABLE(SDO_UTIL.GETVERTICES(Tb.hydrant_coord)) t  ";

			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
         	ResultSet rs = s.executeQuery(sql);
            while(rs.next())
			 {
			   arrayset.add(rs.getInt(1));   //Adds t.x
               arrayset.add(rs.getInt(2));  //Adds t.y
             }
		}
        catch(Exception e)
	    {
	    	System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }
    return arrayset;
    }
    
  //Function to Get the coordinates of buildings for range query
    public ArrayList<Integer> range_vertbuilding(ArrayList<Integer> al)
        {
             p.connectDB();
                                   
             String s1 = "";
             for(int i=0;i<al.size();i++)
             {
                 s1+=al.get(i)+",";
             }
             s1+=al.get(0)+","+al.get(1);
                                    
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
          
         try
		 {	String sql = "SELECT t.X,t.Y FROM building B, TABLE(SDO_UTIL.GETVERTICES(B.building_shape)) t "
                        + "WHERE sdo_relate(B.building_shape, "
                        + "SDO_Geometry (2003, null, null,SDO_ELEM_INFO_ARRAY(1,1003,1),"
                        + "SDO_ORDINATE_ARRAY("+s1+")), "
                        + "'mask=ANYINTERACT') = 'TRUE'";

		    Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
  			ResultSet rs = s.executeQuery(sql);
            while(rs.next())
			{
                arrayset.add(rs.getInt(1));   
                arrayset.add(rs.getInt(2));
                           
            }
	     }
         catch(Exception e)
		    {
		    	System.out.println(e.toString());
		    }finally {
		    	if (conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		             }

    return arrayset;
    }
   
    
    //Function to Get the coordinates of fire buildings for range query
    public ArrayList<Integer> range_vertfirebuidling(ArrayList<Integer> al) 
    {
             p.connectDB();         
             String s1 = "";
             for(int i=0;i<al.size();i++)
             {
                 s1+=al.get(i)+",";
             }
             s1+=al.get(0)+","+al.get(1);     
             ArrayList<Integer> arrayset= new ArrayList<Integer>();
          
       try
       {
			String sql = "Select t.X,t.Y FROM building B, TABLE(SDO_UTIL.GETVERTICES(B.building_shape)) t "
                                + "WHERE sdo_relate( b.building_shape,"
                                + "SDO_Geometry (2003, null, null,SDO_ELEM_INFO_ARRAY(1,1003,1),"
                                + "SDO_ORDINATE_ARRAY("+s1+")), "
                                + "'mask=ANYINTERACT') = 'TRUE' and "
                                + "b.building_on_fire ='y' ";
			
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();    
        	ResultSet rs = s.executeQuery(sql);
            while(rs.next())
			{
              arrayset.add(rs.getInt(1));   
              arrayset.add(rs.getInt(2));
                           
            }
		}
       catch(Exception e)
	    {
	    	System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }
    return arrayset;
    }
  
    
    //Function to Get the coordinates of hydrants for range query
    public ArrayList<Integer> range_hydrant(ArrayList<Integer> al)
    {
             p.connectDB();
             String s1 = "";
             for(int i=0;i<al.size();i++)
             {
                 s1+=al.get(i)+",";
             }
             s1+=al.get(0)+","+al.get(1);
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
       try
        {
			String sql = "select t.X,t.Y FROM hydrant h, TABLE(SDO_UTIL.GETVERTICES(h.hydrant_coord)) t "
                                + "WHERE sdo_relate( h.hydrant_coord,"
                                + "SDO_Geometry (2003, null, null,SDO_ELEM_INFO_ARRAY(1,1003,1),"
                                + "SDO_ORDINATE_ARRAY("+s1+")), "
                                + "'mask=ANYINTERACT') = 'TRUE'";
                                

			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(sql);
            while(rs.next())
			{
               arrayset.add(rs.getInt(1));   
               arrayset.add(rs.getInt(2));
                           
            }
		}
       catch(Exception e)
	    {
	    	System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }
         
     return arrayset;
    }
    
    //Function to Get the coordinates of buildings 
    public ArrayList<Integer> neighbour_vertbuilding()
    {
            p.connectDB();
            ArrayList<Integer> arrayset= new ArrayList<Integer>();

       try
		{
			String sql = "SELECT t.X,t.Y FROM building B, building G, TABLE(SDO_UTIL.GETVERTICES(B.building_shape)) t "
                                + "WHERE SDO_WITHIN_DISTANCE "
                                + "(B.building_shape,G.building_shape,'DISTANCE=100')='TRUE' "
                                + "AND G.building_on_fire='y'";
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery(sql);
			while(rs.next())
			{
               arrayset.add(rs.getInt(1));   
               arrayset.add(rs.getInt(2));
     
             }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}

    return arrayset;
    }
  //Function to Get the no:of  coordinates of buildings 
    public ArrayList<Integer> neighbour_numbuilding()
    {
            p.connectDB();
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
      
      try
		{
			String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(B.building_shape)) FROM building B, building G "
                                + "WHERE SDO_WITHIN_DISTANCE "
                                + "(B.building_shape,G.building_shape,'DISTANCE=100')='TRUE' "
                                + "AND G.building_on_fire='y'";

			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery(sql);
			
			while(rs.next())
			{
                arrayset.add(rs.getInt(1));   
  
             }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}

    return arrayset;
    }
  //Function to Get the coordinates of buildings 
    public ArrayList<Integer> closesthydrant_vertbuilding(String c)
    {
            p.connectDB();
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
        try
		{
			String sql = "SELECT t.X,t.Y FROM building B, TABLE(SDO_UTIL.GETVERTICES(B.building_shape)) t "
                                + "WHERE SDO_CONTAINS(b.building_shape, "
                                + "SDO_Geometry (2001,null,"
                                + "SDO_POINT_TYPE("+c+",null),null,null)) = 'TRUE'";
			//sk
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery(sql);
          	while(rs.next())
			{
                 arrayset.add(rs.getInt(1));   
                 arrayset.add(rs.getInt(2));
            }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
    return arrayset;
    }
  //Function to Get the no:of coordinates of buildings 
    public ArrayList<Integer> closesthydrant_numbuilding(String sample)
    {
            p.connectDB();
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
       try
		{
			String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(b.building_shape)) FROM building b "
                                + "WHERE SDO_CONTAINS(b.building_shape,"
                                + " SDO_Geometry (2001,null,"
                                + "SDO_POINT_TYPE("+sample+",null),null,null)) = 'TRUE'";
			//sk
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery(sql);
			while(rs.next())
			{
                            arrayset.add(rs.getInt(1));   
             }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}
    return arrayset;
    }




//Function to Get the number of coordinates of whole region  

    public ArrayList<Integer> wholeRegion_num()
    {
    	p.connectDB();
    	ArrayList<Integer> arrayset= new ArrayList<Integer>();
        
           
        try
		{
		 String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(building_shape)) from building ";
		 Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
         Statement s = conn.createStatement();
	     ResultSet rs = s.executeQuery(sql);
                 
			while(rs.next())
			{
              arrayset.add(rs.getInt(1));   
            }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }

    return arrayset;
    }


 //Function to Get the number of coordinates of fire buildings
    public ArrayList<Integer>  wholeRegion_numfirebuilding()
    {
    	p.connectDB(); 
    	ArrayList<Integer> arrayset= new ArrayList<Integer>();

        try
		{
			String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(building_shape)) from building where building_on_fire='y'";
		    Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();
         	ResultSet rs = s.executeQuery(sql);
            while(rs.next())
		 	{
            	
      			arrayset.add(rs.getInt(1));   

	        	} 
		}
         catch(Exception e){
	    System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }
    return arrayset;
    }

 
    //Function to Get the number of coordinates of buildings for range query
    public ArrayList<Integer> range_numbuilding(ArrayList<Integer> al)
    {
             p.connectDB();
   
             String s1 = "";
             for(int i=0;i<al.size();i++)
             {
                 s1+=al.get(i)+",";
             }
             s1+=al.get(0)+","+al.get(1);

             ArrayList<Integer> arrayset= new ArrayList<Integer>();
            
        try
		  {
		   String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(building_shape)) FROM building B "
                        + "WHERE sdo_relate(B.building_shape, "
                        + "SDO_Geometry (2003, null, null,SDO_ELEM_INFO_ARRAY(1,1003,1),"
                        + "SDO_ORDINATE_ARRAY("+s1+")), "
                        + "'mask=ANYINTERACT') = 'TRUE'";

        	Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement();                     
		    ResultSet rs = s.executeQuery(sql);	
            while(rs.next())
			 {
               arrayset.add(rs.getInt(1));   
             }
                        
          
		   }
        catch(Exception e)
	    {
	    	System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }

    return arrayset;
    }

  
    //Function to Get the number of  coordinates of fire buildings for range query
    public ArrayList<Integer> range_numfirebuilding(ArrayList<Integer> al)
    {
             p.connectDB();
          
             String s1 = "";
             for(int i=0;i<al.size();i++)
             {
                 s1+=al.get(i)+",";
             }
             s1+=al.get(0)+","+al.get(1);
             ArrayList<Integer> arrayset= new ArrayList<Integer>();
            
      try
		{	String sql = "SELECT (SDO_UTIL.GETNUMVERTICES(building_shape)) FROM building B "
                        + "WHERE sdo_relate(B.building_shape, "
                        + "SDO_Geometry (2003, null, null,SDO_ELEM_INFO_ARRAY(1,1003,1),"
                        + "SDO_ORDINATE_ARRAY("+s1+")), "
                        + "'mask=ANYINTERACT') = 'TRUE' and b.building_on_fire='y'";
   
             Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql);
             while(rs.next())
			 {
               arrayset.add(rs.getInt(1));   
             }

		}
      catch(Exception e)
	    {
	    	System.out.println(e.toString());
	    }finally {
	    	if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	             }
    return arrayset;
    }
    
  //Function to Get the coordinates of hydrants 
    public ArrayList<Integer> closesthydrant_hydrant(String sample)
    {
            p.connectDB();
            ArrayList<Integer> arrayset= new ArrayList<Integer>();
     try
		{
			String sql = "SELECT t.X,t.Y FROM hydrant h, TABLE(SDO_UTIL.GETVERTICES(h.hydrant_coord)) t "
                                + " WHERE SDO_NN(h.hydrant_coord,"
                                + " SDO_Geometry (2003,null,null,"
                                + "SDO_ELEM_INFO_ARRAY(1,1003,1),"
                                + "SDO_ORDINATE_ARRAY("+sample+")),'sdo_num_res=1') = 'TRUE'";
			Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@dagobah.engr.scu.edu:1521:db11g","skrishn2","abc");
            Statement s = conn.createStatement(); 
			ResultSet rs = s.executeQuery(sql);
            while(rs.next())
			{
                 arrayset.add(rs.getInt(1));   
                 arrayset.add(rs.getInt(2));
            }
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
		}

    return arrayset;
    }
}
