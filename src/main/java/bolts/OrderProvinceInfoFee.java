package bolts;

import java.util.HashMap;
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

public class OrderProvinceInfoFee extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	Map<String, Integer> provinceTimeInfoFee = new HashMap<String, Integer>();
	Map<String, Integer> provinceMonthInfoFee = new HashMap<String, Integer>();
	static Logger log = Logger.getLogger(OrderProvinceInfoFee.class);

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			String orderType = input.getStringByField(FName.ORDERTYPE.name());
			String realInforFee = input.getStringByField(FName.REALINFORFEE
					.name());
			String province_id = input.getStringByField(FName.PROVINCE_ID
					.name());
			calculateFee(orderType, province_id, realInforFee);
		} catch (IllegalArgumentException e) {
			if (input.getSourceStreamId().equals(StreamId.SIGNAL24H.name())) {
				log.info("24Hour is coming.");
				provinceTimeInfoFee.clear();
				provinceMonthInfoFee.clear();
			}
		}
		collector.ack(input);
	}

	private void calculateFee(String orderType, String province_id,
			String realInforFee) {
		int inforFee = 0;
		int type = 0;
		if (!orderType.equalsIgnoreCase("")) {
			type = Integer.valueOf(orderType);
		}
		if (!realInforFee.equalsIgnoreCase("")) {
			inforFee = Integer.valueOf(realInforFee);
		}
		if ((type == 4) || (type == 5)) {
			Integer inforFeeCurrent = calculateInforFee(province_id, inforFee,
					provinceMonthInfoFee);
			provinceMonthInfoFee.put(province_id, inforFeeCurrent);
			collector.emit(new Values(orderType, province_id, inforFeeCurrent));
		} else {
			Integer inforFeeCurrent = calculateInforFee(province_id, inforFee,
					provinceTimeInfoFee);
			provinceTimeInfoFee.put(province_id, inforFeeCurrent);
			collector.emit(new Values(orderType, province_id, inforFeeCurrent));
		}
	}

	private Integer calculateInforFee(String province_id, int inforFee,
			Map<String, Integer> infoFeeMap) {
		Integer inforFeeCurrent = infoFeeMap.get(province_id);
		if (inforFeeCurrent == null) {
			inforFeeCurrent = 0;
		}
		inforFeeCurrent += inforFee;
		return inforFeeCurrent;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(FName.ORDERTYPE.name(), FName.PROVINCE_ID
				.name(), FName.REALINFORFEE.name()));
	}

	public void cleanup() {
	}
}
