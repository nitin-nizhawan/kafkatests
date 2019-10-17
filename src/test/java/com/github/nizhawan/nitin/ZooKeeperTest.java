package com.github.nizhawan.nitin;

import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.net.Socket;

import static junit.framework.TestCase.assertTrue;

public class ZooKeeperTest {

    @Rule
    public Network network = Network.newNetwork();

    @Rule
    public GenericContainer zookeeper = new GenericContainer<>("zookeeper:3.4")
            .withNetwork(network)
            .withNetworkAliases("zoo")
            .withLogConsumer(new StdOutConsumer())
            .withExposedPorts(2181);


    @Test
    public void checkZookeeperIsUp() throws Exception {

        int port = zookeeper.getMappedPort(2181);

        String response = netcat("localhost", port, "srvr");
        System.out.println(response);
        assertTrue("response contains zookeeper version ", response.contains("Zookeeper version: 3.4"));
    }


    private String netcat(String host, int port, String msg) throws IOException {
        Socket socket = new Socket(host, port);
        socket.getOutputStream().write(msg.getBytes());
        byte[] responseBytes = new byte[1024];
        int len = socket.getInputStream().read(responseBytes);
        String response = new String(responseBytes, 0, len);
        socket.close();
        return response;
    }
}
