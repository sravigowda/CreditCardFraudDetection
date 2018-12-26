import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.google.gson.Gson;

import scala.math.BigInt;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class FraudAnalysis {

	/*
	 * Following class is used to store the transaction details coming from Kafka
	 * Stream
	 */
	public class JsonTransaction implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String card_id;
		private String member_id;
		private String amount;
		private String postcode;
		private String pos_id;
		private String transaction_dt;

		public String getMember_id() {
			// TODO Auto-generated method stub
			return member_id;
		}

		public String getcard_id() {
			return card_id;
		}

		public String getamount() {
			return amount;
		}

		public String getpostcode() {
			return postcode;
		}

		public String getpos_id() {
			return pos_id;
		}

		public String gettransaction_dt() {
			return transaction_dt;
		}

	}

	/*
	 * Creating an HbaseConnection for connecting to Hbase and running our queries.
	 * We are using only one connection to run all our queries.
	 */
	static Admin hBaseAdmin = HbaseConnection();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		/*
		 * Declaring a dateFormat for the transaction dates that we see in transaction
		 * records
		 */
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

		/*
		 * We are using a unique ID for the group id of Kafka connection. Reusing the
		 * old ID will not fetch data from beginning. This is done to run program
		 * several time
		 */
		String unique = UUID.randomUUID().toString();
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", unique);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList("transactions-topic-verified");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		/*
		 * Converting the stream from kafka to Dstream
		 */
		JavaDStream<String> newDstream = stream.map(x -> x.value());
		// newDstream.print();
		ArrayList<JsonTransaction> list = new ArrayList<JsonTransaction>();

		JavaDStream<JsonTransaction> Close_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonTransaction>() {
			private static final long serialVersionUID = 1L;

			int count = 0;
			DistanceUtility disUtil = new DistanceUtility();

			public Iterator<JsonTransaction> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();

				Gson gson = new Gson();

				try {
					Object obj = jsonParser.parse(x);
					JsonTransaction convertstock = gson.fromJson(obj.toString(), JsonTransaction.class);
					System.out.println("Current transaction card ID is -->" + convertstock.getcard_id());
					System.out.println("Current transaction date is --> " + convertstock.gettransaction_dt());
					// Date date = dateFormat.parse(convertstock.transaction_dt);
					// System.out.println(date.getTime());
					/*
					 * Calling HbaseDao function to check whether transaction is genuine or not
					 */
					int status = HbaseDao(convertstock);
					if (status == 1) {
						System.out.println("This transactions is GENUINE");
					} else {
						System.out.println("This transaction is FRAUD");
					}
					// System.out.println(member_score);
					list.add(convertstock);
					count = count + 1;
					System.out.println(count);

				} catch (Exception e) {
					e.printStackTrace();
				}

				return list.iterator();
			}

		});

		for (JsonTransaction transaction : list) {
			System.out.println(transaction.member_id);
		}

		System.out.println("Inside Dstream " +Close_Dstream.toString());
		JavaDStream<Long> count_stream = Close_Dstream.count();
		System.out.print("Number of RDD in this stream = ");
		count_stream.print();

		jssc.start();
		// / Add Await Termination to respond to Ctrl+C and gracefully close Spark
		// Streams
		jssc.awaitTermination();

	}

	public static Admin HbaseConnection() {

		final long serialVersionUID = 1L;
		HBaseAdmin hbaseAdmin = null;
		Connection con = null;

		/*
		 * Providing all details like Hbase Master IP, Zookeeper IP, Client port to
		 * create an HBase connection. We use it for all our
		 */
		org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration.create();
		conf.setInt("timeout", 1200);
		// conf.set("hbase.master", "ec2-54-158-19-60.compute-1.amazonaws.com:60000");
		// conf.set("hbase.zookeeper.quorum",
		// "ec2-54-158-19-60.compute-1.amazonaws.com");
		conf.set("hbase.master", "10.106.154.5:60000");
		conf.set("hbase.zookeeper.quorum", "10.106.154.5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			if (hbaseAdmin == null) {
				hbaseAdmin = (HBaseAdmin) con.getAdmin();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return hbaseAdmin;

	}

	private static int HbaseDao(JsonTransaction convertstock) throws IOException {

		HTable table = null;
		HTable transactions_table = null;
		HTable lookup_table = null;
		JsonTransaction newconvertstock = convertstock;
		System.out.println(newconvertstock.gettransaction_dt());
		
		// Admin hBaseAdmin = HbaseConnection();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

		Get g = new Get(Bytes.toBytes(convertstock.getcard_id()));
		DistanceUtility disUtil = new DistanceUtility();

		/*
		 * We are creating a key for inserting transactions to transaction table. We are
		 * concatenating transaction date, card ID and amount and getting CRC value for
		 * the same. This is being used a key, which will always be unique. I avoided
		 * using Unique ID here, since it gives different value everytime and data keeps
		 * getting inserted even when it is duplicate entry
		 */

		CRC32 crc = new CRC32();
		Long card_id_int = Long.parseLong(convertstock.getcard_id());
		Double Amount = Double.parseDouble(convertstock.getamount());
		String currentdate = (String) convertstock.gettransaction_dt();
		
		// System.out.println("Card Id in Integer form is --> " +card_id_int);
		Long transaction_dt_int = null;

		try {
			transaction_dt_int = dateFormat.parse(convertstock.gettransaction_dt()).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String crc_string = card_id_int.toString() + Amount.toString() + transaction_dt_int.toString();
		crc.update(crc_string.getBytes());
		String Unique_id = Long.toString(crc.getValue());
		/*
		 * Declaring Put class to insert and update transaction details in transaction
		 * table and master look up table Inserting transaction data, as explained
		 * earlier using crc32 value as key Updating lookup table, card id is being used
		 * as key
		 */
		
		//Put p = new Put(Bytes.toBytes(crc.getValue()));
		Put p = new Put(Bytes.toBytes(Unique_id));
		Put u = new Put(Bytes.toBytes(convertstock.getcard_id()));

		/*
		 * Instantiating the HTable class. This can be used to communicate with HBase
		 * table. Here table is used to communicate with master_lookup table while
		 * transactions table is used to communicate with transactions table
		 */
		table = new HTable(hBaseAdmin.getConfiguration(), "master_lookup_hbase");
		//table = new HTable(hBaseAdmin.getConfiguration(), "master_lookup_hbase_2");
		transactions_table = new HTable(hBaseAdmin.getConfiguration(), "card_transactions_hbase");
		//transactions_table = new HTable(hBaseAdmin.getConfiguration(), "card_transactions_hbase_2");

		if (table != null) {

			Result result = table.get(g);
			/*
			 * Collecting the values of score, upper card limit, postcode and last
			 * transaction date which is used to validate the transaction
			 */
			byte[] value = result.getValue(Bytes.toBytes("cf10"), Bytes.toBytes("score"));
			byte[] ucl = result.getValue(Bytes.toBytes("cf9"), Bytes.toBytes("limit"));
			byte[] postcode = result.getValue(Bytes.toBytes("cf7"), Bytes.toBytes("postcode"));
			byte[] transaction_date = result.getValue(Bytes.toBytes("cf8"), Bytes.toBytes("transaction_date"));

			if (value != null) {
				table.close();
				// hBaseAdmin.close();
				int limit = Integer.parseInt(Bytes.toString(ucl));
				int score = Integer.parseInt(Bytes.toString(value));
				Date last_date = null;
				Date date = null;
				try {
					date = dateFormat.parse(convertstock.transaction_dt);
					last_date = dateFormat.parse(new String(transaction_date, "UTF8"));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.print("Current transaction date is -->" +currentdate);
				//System.out.println("Last transactions date is -->" +last_date);
				
				/*
				 * These are written for debugging purpose
				System.out.println("New Current transactions date is -->" +date.toString());
				System.out.println("Current transaction time is -->" +date.getTime()/1000);
				System.out.println("Last transaction time is -->" +last_date.getTime()/1000);
				*/
				
				/*
				 * Calculating the time difference between two transactions and getting value
				 * for distance that can be covered in this period.
				 */
				long Difference = (date.getTime() / 1000 - last_date.getTime() / 1000);
				long permitted_distance = Difference / 4;
				// System.out.println("Time Difference between transactions in hours is -->"
				// +Difference/3600);

				String code = new String(postcode, "UTF-8");

				/*
				 * Finding out the distance between postcode of current transaction and previous
				 * transactions. This will help us to identify whether given transaction is
				 * fraud or not.
				 */
				// System.out.println("The code value is -> " +code);
				double dist = disUtil.getDistanceViaZipCode(code, convertstock.getpostcode().toString());

				// System.out.println("Distance is --> " +disUtil.getDistanceViaZipCode(code,
				// convertstock.getpostcode().toString()));
				/*
				 * We are adding all values from transaction details to transaction details
				 * table.
				 */
				
				//System.out.println("Current transaction date is "+convertstock.gettransaction_dt());
				
				p.add(Bytes.toBytes("cf1"), Bytes.toBytes("card_id"), Bytes.toBytes(convertstock.getcard_id()));
				p.add(Bytes.toBytes("cf2"), Bytes.toBytes("member_id"), Bytes.toBytes(convertstock.getMember_id()));
				p.add(Bytes.toBytes("cf3"), Bytes.toBytes("Amount"), Bytes.toBytes(convertstock.getamount()));
				p.add(Bytes.toBytes("cf4"), Bytes.toBytes("postcode"), Bytes.toBytes(convertstock.getpostcode()));
				p.add(Bytes.toBytes("cf5"), Bytes.toBytes("pos_id"), Bytes.toBytes(convertstock.getpos_id()));
				p.add(Bytes.toBytes("cf6"), Bytes.toBytes("transaction_dt"),Bytes.toBytes(convertstock.gettransaction_dt().toString()));
				
				

				if ((Double.parseDouble(convertstock.getamount()) < limit) && (score > 200) && (dist < permitted_distance)) {
					System.out.println("Limit = " + limit + " score = " + score + " distance =" + dist);
					/*
					 * If transaction is genuine we are inserting transaction as GENUINE and
					 * updating look up table
					 */
					String status = "GENUINE";
					p.add(Bytes.toBytes("cf7"), Bytes.toBytes("status"), Bytes.toBytes(status));
					transactions_table.put(p);
					
					u.add(Bytes.toBytes("cf8"), Bytes.toBytes("transaction_date"),
							Bytes.toBytes(convertstock.gettransaction_dt().toString()));
					u.add(Bytes.toBytes("cf7"), Bytes.toBytes("postcode"),
							Bytes.toBytes(convertstock.getpostcode().toString()));
					table.put(u);
					return 1;
				} else {
					/*
					 * If transaction is fraud then we are updating only transaction table
					 */
					String status = "FRAUD";
					p.add(Bytes.toBytes("cf7"), Bytes.toBytes("status"), Bytes.toBytes(status));
					transactions_table.put(p);
					return 0;
				}
			} else {
				System.out.println("Null value received");

			}
		}
		table.close();
		// hBaseAdmin.close();
		return 0;
	}

}
