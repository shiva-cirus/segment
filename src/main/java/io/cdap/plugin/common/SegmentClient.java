package io.cdap.plugin.common;

import com.jakewharton.retrofit.Ok3Client;
import com.segment.analytics.Analytics;
import com.segment.analytics.messages.IdentifyMessage;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SegmentClient {

  private static Map<String,SegmentClient> segmentClientMap = new HashMap<String,SegmentClient>();
  private Analytics analytics;
  private static final Logger LOG = LoggerFactory.getLogger(SegmentClient.class);

  private SegmentClient(String writeKey, int connectTimeout, int readTimeout, int writeTimeout  ){
    LOG.debug("Creating Segment Analytics client with Key "+writeKey);
     analytics =
      Analytics.builder(writeKey)
        .client(createClient(connectTimeout,readTimeout,writeTimeout))
        .build();
  }

  public static SegmentClient getInstance(String writeKey, int connectTimeout, int readTimeout, int writeTimeout){


    if (segmentClientMap.containsKey(writeKey)){
      LOG.debug("Reusing Segment Analytics client with Key "+writeKey);
      return segmentClientMap.get(writeKey);
    }
    SegmentClient instance = new SegmentClient(writeKey,connectTimeout,readTimeout,writeTimeout);
    segmentClientMap.put(writeKey,instance);
    return instance;
  }

  private static Client createClient(int connectTimeout, int readTimeout, int writeTimeout ) {
    return new Ok3Client(
      new OkHttpClient.Builder()
        .connectTimeout(connectTimeout, TimeUnit.SECONDS)
        .readTimeout(readTimeout, TimeUnit.SECONDS)
        .writeTimeout(writeTimeout, TimeUnit.SECONDS)
        .build());
  }

  public void identify(String userId, Map<String,String> traits, Map<String,String> context){
    analytics.enqueue(IdentifyMessage.builder()
                        .userId(userId)
                        .traits(traits)
                        .context(context)
                      );

  }

  private void flush()
  {
    analytics.flush();
  }

  public static void flushAll(){

    for (SegmentClient clnt : segmentClientMap.values()) {
      clnt.flush();
    }
  }



}
