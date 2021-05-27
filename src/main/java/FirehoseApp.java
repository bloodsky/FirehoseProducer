import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import utils.URLUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class FirehoseApp {

    public static void main(String[] args) {

        Producer fileOneProducer = new Producer(URLUtils.QUERY_1_FILE_1,URLUtils.STREAM_1);
        fileOneProducer.start();
        Producer fileTwoProducer = new Producer(URLUtils.QUERY_1_FILE_2,URLUtils.STREAM_2);
        fileTwoProducer.start();

    }

    static class Producer implements Runnable {

        Thread pthread;
        // Client
        AmazonKinesisFirehose fhClient = FHClient.getClient();
        private final String URL;
        private final String STREAM;

        Producer(String url, String stream) {
            URL = url;
            STREAM = stream;
        }
        @Override
        public void run() {
            try {
                dataCrawl();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void start() {
            System.out.println("Thread started");
            if (pthread == null) {
                pthread = new Thread(this, STREAM);
                pthread.start();
            }
        }

        public void dataCrawl() throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            URL url = new URL(URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            if (connection.getResponseCode() == 200) {
                try (InputStreamReader streamReader = new InputStreamReader(connection.getInputStream());
                     BufferedReader br = new BufferedReader(streamReader);
                     Stream<String> lines = br.lines().skip(1)) {
                    // Header jumping
                    lines.forEach(s -> sb.append(s).append("\n"));
                }
            }
            sendData(sb.toString().trim());
        }

        public void sendData(String data) throws InterruptedException {
            // Create records
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(STREAM);
            putRecordRequest.setRecord(new Record().withData(ByteBuffer.wrap(data.getBytes())));
            // Send data
            PutRecordResult result = fhClient.putRecord(putRecordRequest);
            System.out.println(result.toString());
            Thread.sleep(5000);
        }
    }
}
