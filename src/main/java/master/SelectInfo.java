package master;

import java.util.List;
import java.util.Objects;

public class SelectInfo {
    boolean isValid = false;
    String ip = null;
    String sql = null;

    public SelectInfo(boolean isValid, String ip, String sql) {
        this.isValid = isValid;
        if(this.isValid){
            this.ip = ip;
            this.sql = sql;
        }
    }

    public SelectInfo(boolean isValid) {
        this.isValid = isValid;
    }

    //TODO:client调用这个构造函数，然后判断isValid、取信息等等
    public SelectInfo(String str) {
        String[] tokens = str.split(" ",3);
        if(Objects.equals(tokens[0], "true")){
            this.isValid = true;
            this.ip = tokens[1];
            this.sql = tokens[2];
        }else{
            this.isValid = false;
        }
    }

    public String Serialize(){
      String ret = "";
      if(this.isValid){
          ret += "true " + ip + " " + sql;
      }else{
          ret += "false";
      }
      return ret;
    }
}