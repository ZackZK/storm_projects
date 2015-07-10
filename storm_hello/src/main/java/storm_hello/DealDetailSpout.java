package storm_hello;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class DealDetailSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	private Jedis jedis;
	private String host;
	private int port;
	private final String key = "tradeResult";

	public DealDetailSpout(String host, int port) {
		this.host = host;
		this.port = port;
	}
	public void nextTuple() {
		String content = jedis.rpop(key);
		if (content == null || "nil".equals(content)) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
			}
		} else {
			content += "_DealDetail";
			System.out.println(content);
			_collector.emit(new Values(content));
		}
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		connectToRedis();
		this._collector = arg2;
	}
	
	private void connectToRedis() {
		jedis = new Jedis(host, port);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("DealDetail"));
	}

}
