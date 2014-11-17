package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import db.DB;

import util.FName;
import util.StreamId;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class OrderTotalInfoFeeDB extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	static Integer timeOrderFee = new Integer(0);
	static Integer monthOrderFee = new Integer(0);
	private String tableNameByTime;
	private String tableNameByMonth;
	private DB db = new DB();
	static Logger log = Logger.getLogger(OrderTotalInfoFeeDB.class);

	public OrderTotalInfoFeeDB(String tableNameByTime, String tableNameByMonth) {
		this.tableNameByTime = tableNameByTime;
		this.tableNameByMonth = tableNameByMonth;
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String orderType = null;
		try {
			orderType = input.getStringByField(FName.ORDERTYPE.name());
		} catch (IllegalArgumentException e) {
		}
		if (orderType != null) {
			Integer orderFee = input.getIntegerByField(FName.REALINFORFEE
					.name());
			int type = 0;
			if (!orderType.equalsIgnoreCase("")) {
				type = Integer.valueOf(orderType);
			}
			if ((type == 4) || (type == 5)) {
				monthOrderFee = orderFee;
			} else {
				timeOrderFee = orderFee;
			}
		} else {
			if (input.getSourceStreamId().equals(StreamId.SIGNALDB.name())) {
				String action = input.getStringByField(FName.ACTION.name());
				String timePeriod = action.trim();
				downloadToDB(timePeriod);
			} else if (input.getSourceStreamId().equals(
					StreamId.SIGNAL24H.name())) {
				log.info("24Hour is coming.");
				timeOrderFee = 0;
				monthOrderFee = 0;
			}
		}
		collector.ack(input);
	}

	private void downloadToDB(String timePeriod) {
		db.insertUser(tableNameByTime, timePeriod, timeOrderFee.toString());
		db.insertUser(tableNameByMonth, timePeriod, monthOrderFee.toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void cleanup() {
	}
}
