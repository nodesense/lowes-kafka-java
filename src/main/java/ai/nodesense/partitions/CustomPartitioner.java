package ai.nodesense.partitions;




import java.util.List;
import java.util.Map;
import java.util.Random;

import io.confluent.common.utils.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;


public class CustomPartitioner implements Partitioner {
   // private IUserService userService;
     private Random random;

    public CustomPartitioner() {
       // userService = new UserServiceImpl();
        random = new Random();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {

        int partition = 0;

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        int numPartitions = partitions.size();

        System.out.println("Total partitions " + partitions.size() + " for topic");


        if (numPartitions <= 1) {
            return 0;
        }

        String userName = (String) key;

        if (userName.equals(("OursSpecificKey"))) {
            return 0;
        }

        // Find the id of current user based on the username
        //Integer userId = userService.findUserId(userName);
        Integer userId = random.nextInt(5);
        // If the userId not found, default partition is 0
        if (userId != null) {
            partition = userId;
        }

        // or use hash key
        // -1 does ensure that 0 is not taken
        partition = Math.abs(Utils.murmur2(userName.getBytes()) % (numPartitions - 1)) + 1;

        // Use custom models like Sensor key, product id etc for partition



        System.out.println(" For key " + key + " Part " + partition);
        return partition;
    }

    @Override
    public void close() {

    }

}