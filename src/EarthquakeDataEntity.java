import org.yecht.Data;

import java.util.Date;

public class EarthquakeDataEntity {
	String StationId;
	String PointId;
	String ItemId;
	String Flag;
	Long   Timestamp;
	float ObsValue;
	public String getStationId() {
		return StationId;
	}
	public void setStationId(String stationId) {
		StationId = stationId;
	}
	public String getPointId() {
		return PointId;
	}
	public void setPointId(String pointId) {
		PointId = pointId;
	}
	public String getItemId() {
		return ItemId;
	}
	public void setItemId(String itemId) {
		ItemId = itemId;
	}
	public Long getTimestamp() {
		return Timestamp;
	}
	public void setDate(Long timestamp) {
		this.Timestamp = timestamp;
	}
	public float getObsValue() {
		return ObsValue;
	}
	public void setObsValue(float obsValue) {
		ObsValue = obsValue;
	}
	public String getFlag() {
		return Flag;
	}
	public void setFlag(String flag) {
		Flag = flag;
	}
	

}
