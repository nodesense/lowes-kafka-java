package ai.nodesense.zookeeper;

import java.util.ArrayList;
import java.util.List;

import kafka.utils.ZkUtils;
import org.apache.zookeeper.ZooKeeper;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.security.auth.SecurityProtocol;


import scala.collection.JavaConversions;
import scala.collection.Seq;

import org.I0Itec.zkclient.ZkClient;

public class BrokersInfo {



    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, null);
        List<String> ids = zk.getChildren("/brokers/ids", false);
        for (String id : ids) {
            System.out.println("ID " + id);
            String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
            System.out.println(id + ": " + brokerInfo);
        }
    }
}
