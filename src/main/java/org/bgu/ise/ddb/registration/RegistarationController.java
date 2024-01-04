/**
 *
 */

package org.bgu.ise.ddb.registration;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.time.LocalDate;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.mongodb.*;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{


	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		boolean result = false;
		try {
			try {
				result = isExistUser(username);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (result) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}
			else {
				MongoClient mongoClient = new MongoClient("localhost", 27017);
				MongoDatabase db = mongoClient.getDatabase("BigDataProject");
				MongoCollection<Document> users = db.getCollection("Users");
				Document user = new Document();
				user.append("Username", username);
				user.append("password", password);
				user.append("firstName", firstName);
				user.append("lastName", lastName);
				LocalDate currentDate = LocalDate.now();
				user.append("registration_date", currentDate);
				users.insertOne(user);
				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
				mongoClient.close();
			}
		}
		catch(MongoException e) {
			e.printStackTrace();
		}
	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			@SuppressWarnings("deprecation")
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users"); // get Users collection
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("Username", username);
			DBCursor cursor = users.find(myQuery);//Send query to Mongo, get the results

			if (cursor.count() != 0) {
				result =  true;
			}
			mongoClient.close();
		}

		catch(MongoException e) {
			e.printStackTrace();
		}
		return result;
	}


	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		try {
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			@SuppressWarnings("deprecation")
			DB db = mongoClient.getDB("BigDataProject");
			DBCollection users = db.getCollection("Users"); // get Users collection
			BasicDBObject myQuery = new BasicDBObject();
			myQuery.put("Username", username);
			myQuery.put("password", password);
			DBCursor cursor = users.find(myQuery);//Send query to Mongo, get the results

			if (cursor.count() != 0) {
				result =  true;
				
			}
			mongoClient.close();
		}
		catch(MongoException e) {
			e.printStackTrace();
		}


		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		long result = 0;
		try {


			LocalDate currentDate = LocalDate.now();
			LocalDate startDate = currentDate.minusDays(days);
			currentDate = currentDate.plusDays(1);
			Bson filter = Filters.and(
					Filters.gte("registration_date", startDate),
					Filters.lt("registration_date", currentDate));
			MongoClient mongoClient = new MongoClient("localhost", 27017); 
			MongoDatabase database = mongoClient.getDatabase("BigDataProject");
			MongoCollection<Document> users = database.getCollection("Users");

			// Count the number of documents matching the filter
			 result = users.countDocuments(filter);
			mongoClient.close();
		}
		catch (MongoException e) {
			e.printStackTrace();
			
		}
		return (int)result;
		}


		/**
		 * The function retrieves all the users
		 * @return
		 */
		@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
		@ResponseBody
		@org.codehaus.jackson.map.annotate.JsonView(User.class)
		public  User[] getAllUsers(){
			User[] users_list = null;
			try {


				MongoClient mongoClient = new MongoClient("localhost", 27017);
				@SuppressWarnings("deprecation")
				DB db = mongoClient.getDB("BigDataProject");
				DBCollection users = db.getCollection("Users"); // get Users collection

				DBCursor cursor = users.find();
				int size = cursor.size();
				User[] users_List = new User[size];
				int i = 0;
				while (cursor.hasNext()) {
					DBObject dbObject = cursor.next();
					User user = new User(
							dbObject.get("Username").toString(),
							dbObject.get("firstName").toString(),
							dbObject.get("lastName").toString()
							);
					users_List[i] = user;
					i++;
					System.out.println(user);
				}
				mongoClient.close();
				return users_List;}
			catch (MongoException e) {
				e.printStackTrace();
			} 
		return users_list;
	}}