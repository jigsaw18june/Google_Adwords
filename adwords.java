import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class adwords
{
	private  String[] outputFile;
	private  String[] fileName;
	private String userName;
	private String password;
	private int[] kValue;

	public adwords()
	{
		outputFile = new String[6];
		fileName = new String[4];
		kValue = new int[6];
	}

	public static void main(String args[]) throws SQLException
	{

		adwords db = new adwords();
		Connection conn = null;
		db.readCredentialsAndData("system.in");
		
		// file handling input
		db.fileName[0] = "Keywords.dat";
		db.fileName[1] = "Advertisers.dat";
		db.fileName[2] = "Queries.dat";
		db.fileName[3] = "system.in";
		db.outputFile[0] = "system.out.1";
		db.outputFile[1] = "system.out.2";
		db.outputFile[2] = "system.out.3";
		db.outputFile[3] = "system.out.4";
		db.outputFile[4] = "system.out.5";
		db.outputFile[5] = "system.out.6";
		
		// input end

		try
		{

			String url = "jdbc:oracle:thin:@oracle.cise.ufl.edu:1521:orcl";
			
			conn = db.connect(url, db.userName, db.password);
			
			db.importData(conn);
			// call to write to files
			db.createOutputFile(db.outputFile);
			int k = 0;
			while(k < 6)
			{
				db.writeData(conn, db.outputFile[k], "outputlist" + (k+1));
				k++;
			}
			// file data write end
			
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			if (conn != null)
				conn.close();
		}
	}
	
	private void importData(Connection conn) throws SQLException
	{
		if (conn != null)
		{
			try
			{
				String sqlurl =  "sqlplus" + " " + userName + "/" + 
						password + "@orcl" + " " +
						"@tryInitSingle2.sql";
				Process p = Runtime.getRuntime().exec(sqlurl);
				p.waitFor();
				String loaderurl = "sqlldr" + " " + userName + "/" +
						password + "@orcl";

				Process p2 = Runtime.getRuntime().exec(loaderurl + " " + "control=adv2");
				p2.waitFor();
				Process p3 = Runtime.getRuntime().exec(loaderurl + " " + "control=key");
				p3.waitFor();
				Process p4 = Runtime.getRuntime().exec(loaderurl + " " + "control=query");
				p4.waitFor();
				
				//System.out.println("data loaded");

				String createView = "CREATE VIEW AdvKeyCount (AdvertiserId, KeywordCount) AS " +
									"SELECT A.AdvertiserId, count(*) " +
									"FROM Keywords K, Advertisers A " +
									"WHERE A.AdvertiserId = K.AdvertiserId "+
									"GROUP BY A.AdvertiserId";
				
				Statement st = conn.createStatement();
				st.executeUpdate(createView);
				CallableStatement callableStatement1 = null;
				int countrow = 0;
				String count = "Select count(*) as count from Queries";
				ResultSet rset = st.executeQuery(count);
				while (rset.next())
					countrow = rset.getInt("count");
				// put tokens in tokens table
				String calc = "{call calculateRank(?,?,?,?,?,?,?)}";
				callableStatement1 = conn.prepareCall(calc);
				callableStatement1.setInt(1, countrow);
				callableStatement1.setInt(2, kValue[0]);
				callableStatement1.setInt(3, kValue[1]);
				callableStatement1.setInt(4, kValue[2]);
				callableStatement1.setInt(5, kValue[3]);
				callableStatement1.setInt(6, kValue[4]);
				callableStatement1.setInt(7, kValue[5]);
				callableStatement1.execute();
				callableStatement1.close();
				st.close();
				conn.commit();
				//System.out.println("done creating views and tables final");
			} catch (Exception e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn.rollback();
			}
		}
        
        
	}

	private Connection connect(String db, String userid, String pass)
	{
		Connection conn;
		try
		{
			conn = DriverManager.getConnection(db, userid, pass);
		} catch (SQLException e)
		{
			e.printStackTrace();
			System.out.println("Invalid username and password");
			conn = null;
		}
		return conn;

	}
	
	private void createOutputFile(String[] outputFile)
	{
		Writer writer = null;
		int i = 0;
		try
		{
			while (i < 6)
			{
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputFile[i]), "utf-8"));
			i++;
			}
		} catch (IOException ex)
		{
			// report
		} finally
		{
			try
			{
				writer.close();
			} catch (Exception ex)
			{
			}
		}
	}
	
	private void writeData(Connection conn, String filename, String tablename)
	{
		FileOutputStream fop = null;
		BufferedWriter writer = null;
		try
		{
			String query = "select * from" + " " + tablename + " " + "order by qid,rank";
			fop = new FileOutputStream(filename,false);
			writer = new BufferedWriter(new OutputStreamWriter(fop));
			Statement select = conn.createStatement();
			ResultSet result = select.executeQuery(query);
			ResultSetMetaData rsmd = result.getMetaData();
			int columnCount = rsmd.getColumnCount();
			while (result.next())
			{
				StringBuilder row = new StringBuilder();
				for (int i = 1; i <= columnCount; i++)
				{
					row.append(result.getObject(i) + ", ");
				}
				row.deleteCharAt(row.length() - 2);
				writer.write(row.toString());
				writer.newLine();
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				if (writer != null)
					writer.close();
				if (fop != null)
					fop.close();
			} catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	private void readCredentialsAndData(String filename)
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(filename));
			String[] lines = new String[8];
			String[] columns = new String[4];
			for (int i = 0; i < 8; i++)
			{
				lines[i]= br.readLine();
			}
			columns = lines[0].split(" ");
			this.userName = columns[2];
			
			columns = lines[1].split(" ");
			this.password = columns[2];
			
			//System.out.println(this.userName + " " + this.password );
			int j = 0;
			for (int i =2; i < 8; i++)
			{
				columns = lines[i].split(" ");
				kValue[j++] = Integer.parseInt(columns[3]);
			}
			
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			try
			{
				br.close();
			} catch (Exception ex)
			{
			}
		}
	}
}
