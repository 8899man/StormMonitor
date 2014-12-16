package bolts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import db.DB;
import db.DBConstant;

import util.FName;
import util.StreamId;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WapDelayRatio extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	private Map<String, Area> delaySum = new HashMap<String, Area>();
	private DecimalFormat dformat = new DecimalFormat("0.00");
	private String tableName;
	private DB db = new DB();
	private static final int[] REGION = { 300, 500, 1000, 3000 };
	private static final Map<String, Region> SPECIALREG = new HashMap<String, Region>();
	private static final String[] PAGE = { "chapterfee.jsp", "view.jsp",
			"search.jsp", "index.jsp" };
	private static final Set<String> PAGESET = new HashSet<String>();
	static Logger log = Logger.getLogger(WapDelayRatio.class);

	public WapDelayRatio(String tableName) {
		this.tableName = tableName;
		ArrayList<Region> sps = new ArrayList<Region>(4);
		for (int i = 0; i < 4; i++) {
			sps.add(new Region());
		}
		for (int i = 0; i < 4; i++) {
			sps.get(0).reg[i] = (int) (REGION[i] / 0.8);
			sps.get(1).reg[i] = (int) (REGION[i] / 0.9);
			sps.get(2).reg[i] = (int) (REGION[i] / 0.8);
			sps.get(3).reg[i] = (int) (REGION[i] / 0.9);
		}
		for (int i = 0; i < 4; i++) {
			SPECIALREG.put(PAGE[i], sps.get(i));
			PAGESET.add(PAGE[i]);
		}
	}

	class Region {
		int[] reg;

		Region() {
			reg = new int[4];
		}
	}

	class Area {
		int[] a;

		Area() {
			a = new int[5];
		}
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			String beartype = input.getStringByField(FName.BEARTYPE.name());
			String pageName = input.getStringByField(FName.PAGENAME.name());
			String deyStr = input.getStringByField(FName.RECORDTIMELEN.name());
			int delay = 0;
			if (!deyStr.equalsIgnoreCase("")) {
				delay = Integer.valueOf(deyStr);
			}
			beartype = build(beartype.trim());
			String key = pageName + "|" + beartype;
			if (PAGESET.contains(pageName)) {
				countSpecial(pageName, key, delay, delaySum);
			} else {
				countDelay(key, delay, delaySum);
			}
		} catch (IllegalArgumentException e) {
			if (input.getSourceStreamId().equals(StreamId.SIGNAL15MIN.name())) {
				String timePeriod = input.getStringByField(FName.ACTION15MIN
						.name());
				delayAverageCalculate(timePeriod);
				clearData();
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		collector.ack(input);
	}

	private void countSpecial(String pageName, String key, int delay,
			Map<String, Area> delaySum) {
		Area area = getDelay(key, delaySum);
		Region region = SPECIALREG.get(pageName);
		if (delay < region.reg[0])
			area.a[0] += delay;
		else if (delay < region.reg[1])
			area.a[1] += delay;
		else if (delay < region.reg[2])
			area.a[2] += delay;
		else if (delay < region.reg[3])
			area.a[3] += delay;
		else
			area.a[4] += delay;
		delaySum.put(key, area);
	}

	private String build(String beartype) {
		if (beartype.equalsIgnoreCase("2"))
			return "2";
		else
			return "3";
	}

	private void countDelay(String key, int delay, Map<String, Area> delaySum) {
		Area area = getDelay(key, delaySum);
		if (delay < REGION[0])
			area.a[0] += delay;
		else if (delay < REGION[1])
			area.a[1] += delay;
		else if (delay < REGION[2])
			area.a[2] += delay;
		else if (delay < REGION[3])
			area.a[3] += delay;
		else
			area.a[4] += delay;
		delaySum.put(key, area);
	}

	private void delayAverageCalculate(String timePeriod) {
		Map<String, String> parameters = db.parameters;
		String fields = parameters.get(tableName);
		try {
			long startTime = System.currentTimeMillis();
			String sql = "insert into " + this.tableName + "(" + fields + ")"
					+ " values(?,?,?,?,?,?,?,?)";
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection(DBConstant.DBURL,
					DBConstant.DBUSER, DBConstant.DBPASSWORD);
			con.setAutoCommit(false);
			PreparedStatement pst = con.prepareStatement(sql);
			for (Map.Entry<String, Area> entry : delaySum.entrySet()) {
				String key = entry.getKey();
				Area area = entry.getValue();
				int sum = area.a[0] + area.a[1] + area.a[2] + area.a[3]
						+ area.a[4];
				String[] delayStr = new String[5];
				for (int i = 0; i < 5; i++) {
					double delay = (double) area.a[i] / sum;
					delayStr[i] = dformat.format(delay);
				}
				String[] words = key.split("\\|", -1);
				pst.setString(1, timePeriod);
				pst.setString(2, words[0]);
				pst.setString(3, words[1]);
				for (int i = 0; i < 5; i++) {
					pst.setString(i + 4, delayStr[i]);
				}
				pst.addBatch();
			}
			pst.executeBatch();
			con.commit();
			pst.close();
			con.close();
			long endTime = System.currentTimeMillis();
			log.info("WapDelayRatio insert takes " + (endTime - startTime)
					+ " ms");
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("insert data to DB is failed.");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error("the class is Not Found!");
		}
	}

	private Area getDelay(String key, Map<String, Area> delayMap) {
		Area area = delayMap.get(key);
		if (area == null)
			area = new Area();
		return area;
	}

	private void clearData() {
		delaySum.clear();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}