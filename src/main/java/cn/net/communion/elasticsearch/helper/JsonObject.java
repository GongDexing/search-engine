package cn.net.communion.elasticsearch.helper;

import java.util.Collection;
import java.util.Map;

import org.json.JSONObject;

public class JsonObject {
    private int errcode;
    private JSONObject data;

    public JsonObject() {
        this.data = new JSONObject();
    }

    public JsonObject setErrcode(int errcode) {
        this.errcode = errcode;
        return this;
    }

    public JsonObject putData(String key, Object value) {
        this.data.put(key, value);
        return this;
    }

    public JsonObject putData(String key, Collection<?> value) {
        this.data.put(key, value);
        return this;
    }

    public JsonObject batchPutData(Map<String, Object> map) {
        map.forEach((key, value) -> {
            putData(key.toString(), value);
        });
        return this;
    }

    public String dataListString(int errcode, Collection<?> list) {
        JSONObject json = this.data.length() > 0 ? new JSONObject() : this.data;
        return json.put("errcode", errcode).put("data", list).toString();
    }

    public String dataObjectString(int errcode, Map<?, ?> map) {
        JSONObject json = this.data.length() > 0 ? new JSONObject() : this.data;
        return json.put("errcode", errcode).put("data", map).toString();
    }

    public String toString() {
        JSONObject json = new JSONObject().put("errcode", this.errcode);
        return data.length() > 0 ? json.put("data", this.data).toString() : json.toString();
    }
}
