package storm_hello;

//import org.zeromq.ZMQ;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TradeDBPubBolt extends BaseBasicBolt {
	private String host;
	private int port;
	private Jedis jedis;
	
	private final String key = "tradeResult";
	
	public TradeDBPubBolt(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	private void connectToRedis() {
		this.jedis = new Jedis(this.host, this.port); 
	}
	
	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		String result = (String) arg0.getValue(0);
		String pub = new String();
		System.out.println("word:" + result);

		pub = result + "_TradeDBPub";
		System.out.println("out=" + pub);
		
		jedis.rpush(key, pub);
//		try {
//			Thread.sleep(3000);
//		} catch (InterruptedException e) {
//			
//			e.printStackTrace();
//		}
		//arg1.emit(new Values(pub));
	}

	
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("tradeDBResult"));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context)  {
		this.connectToRedis();
	}
	
}
