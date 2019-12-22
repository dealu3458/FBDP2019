import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class fileReader {
    private String user_id;
    private String item_id;
    private String cat_id;
    private String merchant_id;
    private String brand_id;
    private String month;
    private String day;
    private String action;
    private String age_range;
    private String gender;
    private String province;

    public fileReader() {}

    public fileReader(String user_id, String item_id, String cat_id,
                      String merchant_id, String brand_id, String month, String day,
                      String action, String age_range, String gender, String province) {
        this.setUserID(user_id);
        this.setItemID(item_id);
        this.setCatID(cat_id);
        this.setMerchantID(merchant_id);
        this.setBrandID(brand_id);
        this.setMonth(month);
        this.setDay(day);
        this.setAction(action);
        this.setAgeRange(age_range);
        this.setGender(gender);
        this.setProvince(province);
    }

    public fileReader(String line) {
        String[] value = line.split(",");
        this.setUserID(value[0]);
        this.setItemID(value[1]);
        this.setCatID(value[2]);
        this.setMerchantID(value[3]);
        this.setBrandID(value[4]);
        this.setMonth(value[5]);
        this.setDay(value[6]);
        this.setAction(value[7]);
        this.setAgeRange(value[8]);
        this.setGender(value[9]);
        this.setProvince(value[10]);
    }

    public String getUserID() {
        return user_id;
    }

    public void setUserID(String user_id) {
        this.user_id = user_id;
    }

    public String getCatID() {
        return cat_id;
    }

    public void setCatID(String cat_id) {
        this.cat_id = cat_id;
    }

    public String getItemID() {
        return item_id;
    }

    public void setItemID(String item_id) {
        this.item_id = item_id;
    }

    public String getMerchantID() {
        return merchant_id;
    }

    public void setMerchantID(String merchant_id) {
        this.merchant_id = merchant_id;
    }

    public String getBrandID() {
        return brand_id;
    }

    public void setBrandID(String brand_id) {
        this.brand_id = brand_id;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getAgeRange() {
        return age_range;
    }

    public void setAgeRange(String age_range) {
        this.age_range = age_range;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String toString() {
        return user_id + "," + item_id + "," + cat_id + "," + merchant_id
                + "," + brand_id + "," + month + "," + day + ","
                + action + "," + age_range + "," + gender + ","
                + province;
    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(user_id);
        out.writeUTF(item_id);
        out.writeUTF(cat_id);
        out.writeUTF(merchant_id);
        out.writeUTF(brand_id);
        out.writeUTF(month);
        out.writeUTF(day);
        out.writeUTF(action);
        out.writeUTF(age_range);
        out.writeUTF(gender);
        out.writeUTF(province);
    }

    public void readFields(DataInput in) throws IOException {
        //TODO Auto-generated method stub
        user_id = in.readUTF();
        item_id = in.readUTF();
        cat_id = in.readUTF();
        merchant_id = in.readUTF();
        brand_id = in.readUTF();
        month = in.readUTF();
        day = in.readUTF();
        action = in.readUTF();
        age_range = in.readUTF();
        gender = in.readUTF();
        province = in.readUTF();
    }
}
