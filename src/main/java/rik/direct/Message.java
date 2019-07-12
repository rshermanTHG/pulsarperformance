package rik.direct;

import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;

public class Message {
    private int ratio;
    private JsonNode body;

    public int getRatio() {
        return ratio;
    }

    public void setRatio(int ratio) {
        this.ratio = ratio;
    }

    public JsonNode getBody() {
        return body;
    }

    public void setBody(JsonNode body) {
        this.body = body;
    }
}
