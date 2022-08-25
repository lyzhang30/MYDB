package top.guoziyang.mydb.client;

import top.guoziyang.mydb.transport.Package;
import top.guoziyang.mydb.transport.Packager;

public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package sendPackage = new Package(stat, null);
        Package receivePackage = rt.roundTrip(sendPackage);
        if(receivePackage.getErr() != null) {
            throw receivePackage.getErr();
        }
        byte[] receivePackageData = receivePackage.getData();
        System.out.println("接收到的结果==>" + receivePackageData);
        return receivePackageData;
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }

}
