import java.io.Serializable;
import java.nio.ByteBuffer;
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

	
	 public static void main(String[] args) throws Exception {
		 
		 

	        Logger.getLogger("org").setLevel(Level.ERROR);
	        Logger.getLogger("akka").setLevel(Level.ERROR);

	        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

	        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");

	        //Admin hbaseAdmin = HbaseConnection();
	        
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
	        int count=0;
	        
	        JavaDStream<JsonTransaction> Close_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonTransaction>() {
				private static final long serialVersionUID = 1L;
				
					int count = 0;
					
				public Iterator<JsonTransaction> call(String x) throws Exception {
					JSONParser jsonParser = new JSONParser();
					
					Gson gson = new Gson();
					

					try {
						Object obj = jsonParser.parse(x);
						//System.out.println(obj.toString());
						JsonTransaction convertstock = gson.fromJson(obj.toString(), JsonTransaction.class);
						System.out.println(convertstock.card_id);
						System.out.println(convertstock.transaction_dt);
						
						Date date = dateFormat.parse(convertstock.transaction_dt);
						System.out.println(date.getTime());
						int member_score = HbaseDao(convertstock,"score");
						System.out.println(member_score);
						//System.out.println(HbaseDao(convertstock));
						list.add(convertstock);
						count = count + 1;
						System.out.println(count);
						
						
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					//System.out.println("Size of List is " +list.size());
					//System.out.println(list.toString());
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

	private static Admin HbaseConnection()   {
		// TODO Auto-generated method stub
		final long serialVersionUID = 1L;
		 Admin hbaseAdmin = null;
		 Connection con=null;
		 //Admin getHbaseAdmin() throws IOException 
		// {
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
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			 try {
				 if (hbaseAdmin == null)
				 //hbaseAdmin = new HBaseAdmin(conf);
				   
				 hbaseAdmin = con.getAdmin(); 
				 //System.out.println("I am inside HbaseConnection");
				 //System.out.println(hbaseAdmin);
			 	} 
			 catch (Exception e) 
			 {
				 e.printStackTrace();
			 }
			 
			 return hbaseAdmin; 
			 
	      }
	
	private static int HbaseDao(JsonTransaction convertstock, String column) throws IOException {
		
		HTable table = null;
		Admin hBaseAdmin = HbaseConnection();
				
		Get g = new Get(Bytes.toBytes(convertstock.getcard_id()));
		//System.out.print("The valueof G is -->");
		//System.out.println(g);
		
		
		table = new HTable(hBaseAdmin.getConfiguration(), "master_lookup_hbase");
		//System.out.print("The valueof Table is -->");
		//System.out.println(table);
		if (table != null) {
			//System.out.println("I am inside hBase table");
			Result result = table.get(g);
			//System.out.print("result returned -->");
			//System.out.println(result);
			byte[] value = result.getValue(Bytes.toBytes("cf10"), Bytes.toBytes(column));
			//System.out.print("value returned -->");
			//System.out.println(value);
			if (value != null) {
				//System.out.println("Value that I am getting is -->" +value);
				table.close();
				hBaseAdmin.close();
				//System.out.println(Integer.parseInt(Bytes.toString(value)));
				return Integer.parseInt(Bytes.toString(value));
				}
			else
			{
				System.out.println("Null value received");
				
			}
		}
		table.close();
		hBaseAdmin.close();
		return -1;
	}
	
	
		 //}
}
