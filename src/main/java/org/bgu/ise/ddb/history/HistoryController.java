/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;

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
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{



	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")  String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);

		try
		{MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase db = mongoClient.getDatabase("BigDataProject");


		MongoCollection<Document> users = db.getCollection("Users");

		Document query = new Document("Username", username);
		long count = users.countDocuments(query);

		MongoCollection<Document> items = db.getCollection("MediaItems");

		Document query2 = new Document("Title", title);
		long count2 = items.countDocuments(query2);


		if (count==0 || count2 == 0) {
			HttpStatus status = HttpStatus.CONFLICT;
			response.setStatus(status.value()); 
		}
		else {
			MongoCollection<Document> history = db.getCollection("History1");
			Date date = new Date();
			Document item1 = new Document();
			item1.append("Username", username);
			item1.append("Title", title);
			item1.append("Timestamp", date.getTime());

			history.insertOne(item1);

			mongoClient.close();
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		}}catch (MongoException e) {
			e.printStackTrace();
		}}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		System.out.println("ByUser ");

		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			MongoDatabase db = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> history = db.getCollection("History1"); 

			Document query = new Document("Username", username);

			// Find documents with the specified username and sort by timestamps in ascending order
			FindIterable<Document> result = history.find(query).sort(descending("Timestamp"));

			// Iterate over the result set
			MongoCursor<Document> iterator = result.iterator();
			long count = history.countDocuments(query);
			HistoryPair[] hispa = new HistoryPair[(int) count];
			int i = 0;
			while (iterator.hasNext()) {
				Document document = iterator.next();
				// Extract the required fields from the document
				String title = document.getString("Title");
				Long timestamp = document.getLong("Timestamp");
				Instant instant = Instant.ofEpochMilli(timestamp);
				Date date = Date.from(instant);
				hispa[i]= new HistoryPair(title, date);
				i++;
			}
			mongoClient.close();
			return hispa;



		}catch (MongoException e) {
			e.printStackTrace();
		}
		return null;}


	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation

		System.out.println("ByItem ");

		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			MongoDatabase db = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> history = db.getCollection("History1"); 

			Document query = new Document("Title", title);

			// Find documents with the specified username and sort by timestamps in ascending order
			FindIterable<Document> result = history.find(query).sort(descending("Timestamp"));

			// Iterate over the result set
			MongoCursor<Document> iterator = result.iterator();
			long count = history.countDocuments(query);
			HistoryPair[] hispa = new HistoryPair[(int) count];
			int i = 0;
			while (iterator.hasNext()) {
				Document document = iterator.next();
				// Extract the required fields from the document
				String user = document.getString("Username");
				Long timestamp = document.getLong("Timestamp");
				Instant instant = Instant.ofEpochMilli(timestamp);
				Date date = Date.from(instant);
				hispa[i]= new HistoryPair(user, date);
				i++;
			}
			mongoClient.close();
			return hispa;



		}catch (MongoException e) {
			e.printStackTrace();
		}
		return null;}



	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation

		try{



			HistoryPair[] list = getHistoryByItems(title);

			MongoClient mongoClient = new MongoClient("localhost", 27017);

			MongoDatabase db = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> userscollection = db.getCollection("Users");

			int size = list.length;

			User[] users = new User[size];
			int i = 0;
			int counter=0;

			while (i<size) {
				Document query = new Document("Username", list[i].getCredentials());
				FindIterable<Document> result = userscollection.find(query);
				MongoCursor<Document> iterator = result.iterator();

				Document document = iterator.next();
				// Extract the required fields from the document
				String username = document.getString("Username");
				String firstname = document.getString("firstName");
				String lastname = document.getString("lastName");
				boolean flag = false;
				for(int j=0;j<size;j++) {
					if (users[j] == null) {
						break;
					}
					if (users[j].getUsername().equals(username) ) {
						flag = true;
					}
				}
				if(!flag) {
					User u = new User(username, firstname, lastname);
					counter++;
					users[i]= u;
				}
				i++;}
			User[] ans = new User[counter];
			for(int j=0;j<counter;j++) {
				ans[j]=users[j];
			}

			mongoClient.close();

			return  ans;}
		catch (MongoException e) {
			e.printStackTrace();}
		return null;}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method=RequestMethod.GET,produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		User[] t1 = getUsersByItem(title1);
		User[] t2 = getUsersByItem(title2);
		double size1 = t1.length;
		double size2 = t2.length;
		double intersection =0;
		for(int i=0;i<size1;i++) {
			for(int j=0;j<size2;j++) {
				if(t1[i].getUsername().equals( t2[j].getUsername())) {
					intersection ++;
				}
			}
		}
		double ret = intersection/(size1+size2-intersection);
		return ret;
	}


}
