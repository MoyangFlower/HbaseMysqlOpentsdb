import java.util.HashMap;

public class OpenTsdbQueryDataEntity {

    public Long getStart() {
        return Start;
    }
    public void setStart(Long start) {
        Start = start;
    }
    public float getValue() {
        return Value;
    }
    public void setValue(float value) {
        Value = value;
    }
    public HashMap<String, String> getQueries() { return Queries; }
    public void setQueries(HashMap<String, String> queries) { Queries=queries;}
    public void setEnd(Long end){End = end;}
    public Long getEnd(){return End;}

    String Metric;
    Long Start;
    Long End;
    String Aggregator;
    float Value;
    HashMap<String, String> Queries;

}
