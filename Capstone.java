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
import org.apache.hadoop.hbase.client.Admin; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.util.Bytes;


//import SparkstreamRSI.JsonStock;

//import SparkstreamRSI.JsonStock;



public class CapStone {
	
	public class JsonTransaction implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String card_id;
		private String member_id;
		private Long amount;
		private Integer postcode;
		private String pos_id;
		private String transaction_dt;
		
		public String getMember_id() {
			// TODO Auto-generated method stub
			return member_id;
		}
		
		public String getcard_id() {
			return card_id;
		}
		
		public Long getamount() {
			return amount;
		}
		
		public Integer getpostcode() {
			return postcode;
		}
		
		public String getpos_id () {
			return pos_id;
		}
		
		public String gettransaction_dt () {
			return transaction_dt;
		}
		
	    }
	
	static Admin hBaseAdmin = HbaseConnection();
	


	
	 public static void main(String[] args) throws Exception {
		 
		 

	        Logger.getLogger("org").setLevel(Level.ERROR);
	        Logger.getLogger("akka").setLevel(Level.ERROR);
	        
	        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

	        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
	        
	   
	        
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

	        JavaDStream<String> newDstream = stream.map(x -> x.value());
	        //newDstream.print();
	        ArrayList<JsonTransaction> list = new ArrayList<JsonTransaction>();
	   
	        
	        JavaDStream<JsonTransaction> Close_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonTransaction>() {
				private static final long serialVersionUID = 1L;
				
					int count = 0;
					DistanceUtility disUtil=new DistanceUtility();
					//Admin hBaseAdmin = HbaseConnection();

					
				public Iterator<JsonTransaction> call(String x) throws Exception {
					JSONParser jsonParser = new JSONParser();
					
					Gson gson = new Gson();
					

					try {
						Object obj = jsonParser.parse(x);
						
						JsonTransaction convertstock = gson.fromJson(obj.toString(), JsonTransaction.class);
						System.out.println("Current transaction card ID is -->" +convertstock.card_id);
						System.out.println("Current transaction date is --> " +convertstock.transaction_dt);
						//Date date = dateFormat.parse(convertstock.transaction_dt);
						//System.out.println(date.getTime());
						int status = HbaseDao(convertstock);
						if (status == 1)
						{
							System.out.println("This transactions is Genuine");
						}
						else
						{
							System.out.println("This transaction is Fraudulent");
						}
						//System.out.println(member_score);
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

	public static  Admin HbaseConnection()  {
		
		final long serialVersionUID = 1L;
		HBaseAdmin hbaseAdmin = null;
		 Connection con=null;
			 org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration.create();
			 conf.setInt("timeout", 1200);
			 //conf.set("hbase.master", "ec2-54-158-19-60.compute-1.amazonaws.com:60000"); 
			 //conf.set("hbase.zookeeper.quorum", "ec2-54-158-19-60.compute-1.amazonaws.com"); 
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
				 if (hbaseAdmin == null)
				 {				   
				 hbaseAdmin = (HBaseAdmin) con.getAdmin(); 
  			 	 } 
			 }
			 catch (Exception e) 
			 {
				 e.printStackTrace();
			 }
			 
			 return hbaseAdmin; 
			 
}
	
	
	
	private static int HbaseDao(JsonTransaction convertstock) throws IOException {
		
		HTable table = null;
		//Admin hBaseAdmin = HbaseConnection();
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
				
		Get g = new Get(Bytes.toBytes(convertstock.getcard_id()));
		DistanceUtility disUtil=new DistanceUtility();

		
		
		table = new HTable(hBaseAdmin.getConfiguration(), "master_lookup_hbase");
		if (table != null) {
						Result result = table.get(g);
			
			byte[] value = result.getValue(Bytes.toBytes("cf10"), Bytes.toBytes("score"));
			byte [] ucl = result.getValue(Bytes.toBytes("cf9"), Bytes.toBytes("limit"));
			byte [] postcode = result.getValue(Bytes.toBytes("cf7"), Bytes.toBytes("postcode"));
			byte [] transaction_date = result.getValue(Bytes.toBytes("cf8"), Bytes.toBytes("transaction_date"));

			
			if (value != null) {
				table.close();
				//hBaseAdmin.close();
				int limit = Integer.parseInt(Bytes.toString(ucl));
				int score = Integer.parseInt(Bytes.toString(value));
				Date last_date = null;
				Date date = null;
				try {
					date = dateFormat.parse(convertstock.transaction_dt);
					last_date = dateFormat.parse(new String(transaction_date,"UTF8"));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.println("Current transaction time is -->" +date.getTime()/1000);
				//System.out.println("Last transaction time is -->" +last_date.getTime()/1000);
				long Difference = (date.getTime()/1000 - last_date.getTime()/1000);
				long permitted_distance = Difference*4;
				//System.out.println("Time Difference between transactions in hours is  -->" +Difference/3600);
				
				
				String code = (Bytes.toString(postcode));
				double dist = disUtil.getDistanceViaZipCode(code, convertstock.getpostcode().toString());
				//System.out.println("Distance is --> " +disUtil.getDistanceViaZipCode(code, convertstock.getpostcode().toString()));
				
				
				if ((convertstock.getamount() < limit) && (score > 200) && (dist < permitted_distance))
				{
					System.out.println("Limit = " +limit + " score = " +score + " distance =" +dist);	
					return 1;
				}
				else
				{
					return 0;
				}
			}
			else
			{
				System.out.println("Null value received");
				
			}
		}
		table.close();
		//hBaseAdmin.close();
		return 0;
	}
	
	
}
