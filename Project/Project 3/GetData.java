import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;



//json.simple 1.1
// import org.json.simple.JSONObject;
// import org.json.simple.JSONArray;

// Alternate implementation of JSON modules.
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData{
	
    static String prefix = "project3.";
	
    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;
	
    // You must refer to the following variables for the corresponding 
    // tables in your database

    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;

    // This is the data structure to store all users' information
    // DO NOT change the name
    JSONArray users_info = new JSONArray();		// declare a new JSONArray

	
    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
	super();
	String dataType = u;
	oracleConnection = c;
	// You will use the following tables in your Java code
	cityTableName = prefix+dataType+"_CITIES";
	userTableName = prefix+dataType+"_USERS";
	friendsTableName = prefix+dataType+"_FRIENDS";
	currentCityTableName = prefix+dataType+"_USER_CURRENT_CITIES";
	hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITIES";
	programTableName = prefix+dataType+"_PROGRAMS";
	educationTableName = prefix+dataType+"_EDUCATION";
	eventTableName = prefix+dataType+"_USER_EVENTS";
	albumTableName = prefix+dataType+"_ALBUMS";
	photoTableName = prefix+dataType+"_PHOTOS";
	tagTableName = prefix+dataType+"_TAGS";
    }
	
	
	
	
    //implement this function

    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException{ 

    	JSONArray users_info = new JSONArray();
		
	// Your implementation goes here....	
		try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY))
		{
			Statement stmt1 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			ResultSet rst = stmt.executeQuery(
				"SELECT USER_ID, FIRST_NAME, LAST_NAME, GENDER, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH " + 
				"FROM " + userTableName + " ");
			while (rst.next())
			{
				JSONObject userInfo = new JSONObject();
				userInfo.put("user_id", rst.getInt(1));
				userInfo.put("first_name", rst.getString(2));
				userInfo.put("last_name", rst.getString(3));
				userInfo.put("gender", rst.getString(4));
				userInfo.put("YOB", rst.getInt(5));
				userInfo.put("MOB", rst.getInt(6));
				userInfo.put("DOB", rst.getInt(7));

				ResultSet rst1 = stmt1.executeQuery(
					"SELECT C.COUNTRY_NAME, C.CITY_NAME, C.STATE_NAME " + 
					"FROM " + cityTableName + " C " + 
					"JOIN " + hometownCityTableName + " HC " + 
					"ON C.CITY_ID = HC.HOMETOWN_CITY_ID " + 
					"WHERE HC.USER_ID = " + rst.getInt(1) + " "
					);

				JSONObject hometownCityInfo = new JSONObject();
				if (!rst1.next())
				{
					userInfo.put("hometown", hometownCityInfo);
				}
				else
				{
					userInfo.put("hometown", hometownCityInfo);
					hometownCityInfo.put("country", rst1.getString(1));
					hometownCityInfo.put("city", rst1.getString(2));
					hometownCityInfo.put("state", rst1.getString(3));
				}


				rst1 = stmt1.executeQuery(
					"SELECT C.COUNTRY_NAME, C.CITY_NAME, C.STATE_NAME " + 
					"FROM " + cityTableName + " C " + 
					"JOIN " + currentCityTableName + " UCC " + 
					"ON C.CITY_ID = UCC.CURRENT_CITY_ID " + 
					"WHERE UCC.USER_ID = " + rst.getInt(1) + " "
					);

				JSONObject currentCityInfo = new JSONObject();
				if (!rst1.next())
				{
					userInfo.put("current", currentCityInfo);
				}
				else
				{
					userInfo.put("current", currentCityInfo);
					currentCityInfo.put("country", rst1.getString(1));
					currentCityInfo.put("city", rst1.getString(2));
					currentCityInfo.put("state", rst1.getString(3));
				}

				rst1 = stmt1.executeQuery(
					"SELECT F.USER2_ID " + 
					"FROM " + userTableName + " U " + 
					"JOIN " + friendsTableName + " F " + 
					"ON U.USER_ID = F.USER1_ID " + 
					"WHERE U.USER_ID = " + rst.getInt(1) + " ");
				JSONArray friendInfo = new JSONArray();
				
				while (rst1.next())
				{
					friendInfo.put(rst1.getInt(1));
				}
				userInfo.put("friends", friendInfo);
				users_info.put(userInfo);
				rst1.close();
			}
			rst.close();
			stmt.close();
			stmt1.close();
		}	


		return users_info;
    }

    // This outputs to a file "output.json"
    public void writeJSON(JSONArray users_info) {
	// DO NOT MODIFY this function
	try {
	    FileWriter file = new FileWriter(System.getProperty("user.dir")+"/output.json");
	    file.write(users_info.toString());
	    file.flush();
	    file.close();

	} catch (IOException e) {
	    e.printStackTrace();
	}
		
    }
}
