package creditscoring;

import java.time.Clock;
import java.util.ArrayList;

/**
 * Created by thinkpad on 2017/12/5.
 */
public class SimpleFlumeEventSender {
    public static void main(String[] args) {
        MyRpcClientFacade client = new MyRpcClientFacade();
        client.init("192.168.1.5", 33333);//flume机器 rpc方式发送数据
        //String sampleData = "Hello Flume!";
        //用户行为数据
        ArrayList<String> arrayList = new ArrayList();
        arrayList.add("1,2,0,3,6,3,9,2,9,2,8,0,2,4,7,8,2,1,4,53");
        arrayList.add("1,2,0,3,6,3,9,2,9,2,8,0,4,4,7,8,2,1,4,53");
        arrayList.add("1,2,0,3,6,3,9,2,9,2,8,0,2,4,7,8,2,1,4,53");
        arrayList.add("1,2,0,3,6,3,9,2,9,2,8,0,2,4,7,8,2,1,4,53");
        while (true) {
            for (int i = 0; i < 10; i++) {
                String datas = arrayList.get((int) (Math.random() * arrayList.size()));
                client.sendDataToFlume(datas);
                System.out.println("==="+datas+"===");
             //   client.cleanUp();
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}