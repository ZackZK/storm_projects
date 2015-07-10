package storm_hello;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class FirstTopo 
{
	private static final String redisHost = "121.41.100.93";
	private static final int redisPort = 6379;
	
    public static void main( String[] args ) throws Exception
    {
    	TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new String1024Spout()).setNumTasks(5);  
        builder.setSpout("DealDetailSpout", new DealDetailSpout(redisHost, redisPort)).setNumTasks(5);
        
        builder.setBolt("StrategeBolt", new StrategeBolt()).shuffleGrouping("spout").shuffleGrouping("DealDetailSpout").setNumTasks(5); 
        builder.setBolt("StockServerBolt", new StockServerBolt()).shuffleGrouping("StrategeBolt").setNumTasks(5);
        builder.setBolt("TradeDBPubBolt", new TradeDBPubBolt(redisHost, redisPort)).shuffleGrouping("StockServerBolt").setNumTasks(5);
        
        Config conf = new Config();  
        conf.setDebug(false); 
        if (args != null && args.length > 0) {  
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(100000000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
        }  
    }
}
