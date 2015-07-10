package storm_hello;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StockServerBolt extends BaseBasicBolt {

	public void execute(Tuple arg0, BasicOutputCollector arg1) {
		String request = (String) arg0.getValue(0);
		String out = new String();
		System.out.println("word:" + request);

		out = request + "_StockServerResult";
		System.out.println("out=" + out);
		arg1.emit(new Values(out));
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("StockServerResult"));
	}

}
