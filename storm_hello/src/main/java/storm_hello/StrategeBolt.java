package storm_hello;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class StrategeBolt extends BaseBasicBolt {

	public void execute(Tuple arg0, BasicOutputCollector arg1) {
        String word = (String) arg0.getValue(0);  
        String out = new String();
        System.out.println("word:" + word);
        
        out = word + "_StrageResult";
        System.out.println("out=" + out);
        arg1.emit(new Values(out));
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("stategeResult"));
	}

}
