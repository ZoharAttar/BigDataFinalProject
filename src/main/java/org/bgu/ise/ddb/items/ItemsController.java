/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.IOException;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import javax.servlet.http.HttpServletResponse;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.*;


import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.Connection;

import java.io.BufferedReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URL;


import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {



	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		String server_name = "132.72.64.124";
		String user_name = "gabyl";
		String password = "BFLit/#L";
		String connectionUrl = "jdbc:sqlserver://" + server_name + ":1433;databaseName=" + user_name + ";user=" + user_name + ";"
				+ "password=" + password + ";encrypt=false;";
		// Define connection to SQL SERVER:
		Statement st = null;
		ResultSet res_set = null;

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			Connection con = DriverManager.getConnection(connectionUrl);

			st = con.createStatement();
			res_set = st.executeQuery("select * from MediaItems");

			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> MediaItems = db.getCollection("MediaItems");

			while (res_set.next()){
				int prod_year = res_set.getInt("PROD_YEAR");
				String title = res_set.getString("TITLE");

				Document item = new Document();
				Document query = new Document();
				query.append("Title", title);
				query.append("Prod_Year", prod_year);

				long count = MediaItems.countDocuments(query);
				if (count == 0) {

				item.append("Title", title);
				item.append("Prod_Year", prod_year);
				MediaItems.insertOne(item);
				}
			}
			mongoClient.close();
			st.close();
			res_set.close();
			con.close();
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());


		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();}
		catch(MongoException e) {
			e.printStackTrace();
		}
	}







	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urlAddress, HttpServletResponse response)
			throws IOException {
		try {
			System.out.println(urlAddress);

			// Create a URL object from the provided address
			URL url = new URL(urlAddress);

			// Open a connection to the remote file
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();

			// Set the request method to GET
			connection.setRequestMethod("GET");

			// Get the input stream from the connection
			InputStream inputStream = connection.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> mediaItems = db.getCollection("MediaItems");

			String line;
			while ((line = reader.readLine()) != null) {
				String[] rowData = line.split(","); // Adjust the delimiter based on your Excel file format

				// Create a document and add each row field
				Document document = new Document();
				Document query = new Document();
				String year = rowData[1];
				int number = Integer.parseInt(year);
				query.append("Title", rowData[0]);
				query.append("Prod_Year", number);
	
				long count = mediaItems.countDocuments(query);
				if (count == 0) {
				document.append("Title", rowData[0]);
				document.append("Prod_Year", number);
				mediaItems.insertOne(document);
				}
			}

			mongoClient.close();
			inputStream.close();

			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		} catch (IOException e) {
			e.printStackTrace();

		} catch (MongoException e) {
			e.printStackTrace();

		}
	}


	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method=RequestMethod.GET,produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")   int topN){
		//:TODO your implementation
		try {

		MongoClient mongoClient = new MongoClient("localhost", 27017);
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB("BigDataProject");
		DBCollection items = db.getCollection("MediaItems"); // get Users collection

		DBCursor cursor = items.find();
		int size = cursor.size();
		MediaItems[] array; 
		if (size >= topN) {
			array = new MediaItems[topN];	
		}
		else {array = new MediaItems[size];
		}

		for (int i = 0; i < array.length; i++)  {
			DBObject dbObject = cursor.next();
			MediaItems item = new MediaItems(
					dbObject.get("Title").toString(),
					Integer.parseInt(dbObject.get("Prod_Year").toString()));
			array[i] = item;
		}
		mongoClient.close();
		return array;
	} catch (MongoException e) {
        e.printStackTrace();
}
		return null;}}

