package iot.data.platform.ingestor.fetcher;

import java.io.IOException;

public interface GtfsRtHttpFetcher {
    byte[] getBytes(String url, UrlProfile urlProfile) throws IOException, InterruptedException;
}