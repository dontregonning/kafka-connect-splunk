package com.splunk.kafka.connect;

import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaHeaderUtility {
    Headers headers;
    String splunkHeaderIndex = "";
    String splunkHeaderHost = "";
    String splunkHeaderSource = "";
    String splunkHeaderSourcetype = "";

    public KafkaHeaderUtility(SinkRecord record) {
        headers = record.headers();
        splunkHeaderIndex = headers.lastWithName("splunk_index").value().toString();
        splunkHeaderHost = headers.lastWithName("splunk_host").value().toString();
        splunkHeaderSource = headers.lastWithName("splunk_source").value().toString();
        splunkHeaderSourcetype = headers.lastWithName("splunk_sourcetype").value().toString();
    }

    public boolean compareRecordHeaders(SinkRecord record) {
        headers = record.headers();

        if(splunkHeaderIndex.equals(headers.lastWithName("splunk_index").value().toString()) &&
                splunkHeaderHost.equals(headers.lastWithName("splunk_host").value().toString()) &&
                splunkHeaderSource.equals(headers.lastWithName("splunk_source").value().toString()) &&
                splunkHeaderSourcetype.equals(headers.lastWithName("splunk_sourcetype").value().toString())) {
            return true;
        }
        return false;
    }

    public Headers getHeaders() {
        return headers;
    }

    public String getSplunkHeaderIndex() {
        return splunkHeaderIndex;
    }

    public String getSplunkHeaderHost() {
        return splunkHeaderHost;
    }

    public String getSplunkHeaderSource() {
        return splunkHeaderSource;
    }

    public String getSplunkHeaderSourcetype() {
        return splunkHeaderSourcetype;
    }
}
