package storm_hello;
import java.util.Map;
import java.util.Random;
import java.util.Date;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StocksSpout extends BaseRichSpout {
	 private SpoutOutputCollector collector;
	 //private static String[] words = {"happy","excited","angry"};
	 //private static String[] words = new String[10];
	 private int count = 0;
	 private final int string_length = 10;
	    
	public void nextTuple() {	
		String word = generateWord();
		collector.emit(new Values(word));
		Utils.sleep(1000); 
	}

	private String generateWord() {
		Random r = new Random();
		StringBuffer wordBuf = new StringBuffer(string_length);;
		char ch = (char)(r.nextInt(26) + 'a');
		
		for(int i=0; i<string_length; i++) {
			wordBuf.append(ch);
		}
		String word = wordBuf.toString();
		
		word += Integer.toString(count++); //加上计数
		
		Date date = new Date();
		long timestamp = date.getTime();
		word += "_"+Long.toString(timestamp); // 加上时间戳
		
		return word;
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("randomstring"));
	}
}
