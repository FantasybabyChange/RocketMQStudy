package com.fantasybaby.rocket.demo;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author: liuxi
 * @time: 2019/4/1 22:33
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("demo");

        // Specify name server addresses.
        producer.setNamesrvAddr("192.168.20.222:9876");
        producer.setRetryTimesWhenSendAsyncFailed(0);
        //Launch the instance.
//        producer.setVipChannelEnabled(false);
        producer.start();

        Message msg = new Message("FantasyBabyTest",
                "TagB",
                "Hello world TAGB".getBytes(RemotingHelper.DEFAULT_CHARSET));
        /*Message msg1 = new Message("TopicTest",
                "TagB",
                "Hello world TAGA".getBytes(RemotingHelper.DEFAULT_CHARSET));*/
        SendResult send = producer.send(msg);
//        SendResult send1 = producer.send(msg1);
        System.out.println(send);
//        System.out.println(send1);
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
    public void testSender() throws InterruptedException, MQClientException {
        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("192.168.20.222:9876");
        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Launch the instance.
         */
        producer.start();

        for (int i = 0; i < 1000; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
                Message msg = new Message("TopicTest" /* Topic */,
                        "TagA" /* Tag */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);

                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
