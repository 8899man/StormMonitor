package bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import util.FName;
import util.StreamId;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OrderTotalInfoFee extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	static Integer timeOrderFee = 0;
	static Integer monthOrderFee = 0;
	static Logger log = Logger.getLogger(OrderTotalInfoFee.class);

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			String orderType = input.getStringByField(FName.ORDERTYPE.name());
			String realInforFee = input.getStringByField(FName.REALINFORFEE
					.name());
			calculateFee(orderType, realInforFee);
		} catch (IllegalArgumentException e) {
			if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
				log.info("24Hour is coming.");
				timeOrderFee = 0;
				monthOrderFee = 0;
			}
		}
		collector.ack(input);
	}

	private void calculateFee(String orderType, String realInforFee) {
		int inforFee = 0;
		int type = 0;
		if (!orderType.equalsIgnoreCase("")) {
			type = Integer.valueOf(orderType);
		}
		if (!realInforFee.equalsIgnoreCase("")) {
			inforFee = Integer.valueOf(realInforFee);
		}
		if ((type == 4) || (type == 5)) {
			monthOrderFee += inforFee;
			collector.emit(new Values(orderType, monthOrderFee));
		} else {
			timeOrderFee += inforFee;
			collector.emit(new Values(orderType, timeOrderFee));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FName.ORDERTYPE.name(), FName.REALINFORFEE
				.name()));
	}

	public void cleanup() {
	}
}
