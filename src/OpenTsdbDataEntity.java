import java.util.HashMap;

public class OpenTsdbDataEntity {
	public String getMetric() {
		return Metric;
	}
	public void setMetric(String metric) {
		Metric = metric;
	}
	public Long getTimestamp() {
		return Timestamp;
	}
	public void setTimestamp(Long timestamp) {
		Timestamp = timestamp;
	}
	public float getValue() {
		return Value;
	}
	public void setValue(float value) {
		Value = value;
	}
	public HashMap<String, String> getTags() {
		return Tags;
	}
	public void setTags(HashMap<String, String> tags) {
		Tags = tags;
	}
	String Metric;
	Long Timestamp;
	float Value;
	 HashMap<String, String> Tags;

}
