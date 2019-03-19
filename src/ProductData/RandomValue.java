package ProductData;

import java.text.DecimalFormat;
import java.util.Random;

/**
 *
 * 返回保留两位小数的随机数
 *
 */

public class RandomValue {
    Random random = new Random();
    DecimalFormat decimalFormat = new DecimalFormat("0.00");
    public String data() {
        String return_value = decimalFormat.format(random.nextFloat()*100);
        return return_value;
    }
}
