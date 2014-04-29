package simpledb.Task1andTests;
import java.sql.*;

import simpledb.remote.SimpleDriver;

//CS4432 Project 1
//Robert Mullins
//Alfred Scott

public class Task1
{
	public static void main(String[] args)
	{
		Connection conn = null;
		try
		{
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();
			
			String s = "create table VET(VId int, VName varchar(8))";
			stmt.executeUpdate(s);
			System.out.println("Table VET created.");
			
			s = "insert into VET(VId, VName) values ";
			String[] vetvals = {"(1, 'Sugar')",
								"(2, 'Spice')"};
			for (int i=0; i<vetvals.length; i++)
				stmt.executeUpdate(s + vetvals[i]);
			System.out.println("VET records inserted."); 
			
			s = "create table DOG(DId int, DName varchar(10), DAge int, Disease varchar(10), VetId int)";
			stmt.executeUpdate(s);
			System.out.println("Table DOG created.");
			
			s = "insert into DOG(DId, DName, DAge, Disease, VetId) values ";
			String[] dogvals = {"(1, 'Frungo', 10, 'none', 1)",
								"(2, 'Duke', 1, 'dog cancer', 1)",
								"(3, 'Spot', 2, 'none', 1)",
								"(4, 'Lt. Dog', 4, 'Dogabetes', 1)",
								"(5, 'Karl Marx', 14, 'communism', 1)",
								"(6, 'Champ', 8, 'none', 2)",
								"(7, 'Dog', 2, 'heart worm', 2)",
								"(8, 'Martin', 3, 'depression', 2)",
								"(9, 'Janet', 10, 'depression', 2)",
								"(10, 'Dianne', 10, 'none', 2)"};
			
			for (int i=0; i<dogvals.length; i++)
			{
			stmt.executeUpdate(s + dogvals[i]);
			}
			System.out.println("DOG records inserted."); 
		 
			
			String dis = "none";
			
			String qry = "select DName from DOG where Disease = '"+ dis + "'";
			ResultSet rs = stmt.executeQuery(qry);
			System.out.println("\nNon-diseased dogs: ");

			while (rs.next()) {
				String DName = rs.getString("DName");
				System.out.println("Name: " +DName);
			}
			rs.close();
			
			String qrya = "select DName, DAge from DOG where DAge = 10";
			//j
			ResultSet rsa = stmt.executeQuery(qrya);
			System.out.println("\nDogs age of 10:");
			while (rsa.next()) {
				String DName = rsa.getString("DName");
				int DId = rsa.getInt("DAge");
				System.out.print("" +
<<<<<<< HEAD
						"Name: "+DName+" Age: "+DId+"\n");
=======
						"Name: "+DName+" Age: "+DId+"");
>>>>>>> 5712dc26842fedf189c49134eb0801334dfdd73a
			}
			
			rsa.close();
		}
		
		catch(SQLException e) 
		{
			e.printStackTrace();
		}
		
		finally 
		{
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}	
	}
}