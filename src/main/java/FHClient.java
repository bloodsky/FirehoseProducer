import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;

public class FHClient {
    public static AmazonKinesisFirehose getClient() {
        return AmazonKinesisFirehoseClientBuilder.standard().withRegion(Regions.US_WEST_1).build();
    }
}
