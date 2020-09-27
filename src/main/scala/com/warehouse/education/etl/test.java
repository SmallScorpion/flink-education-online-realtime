package com.warehouse.education.etl;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.math.BigInteger;
import java.security.MessageDigest;

public class test {
    public static void main(String[] args) throws Exception {
        KuduClient kuduClient = new KuduClient.KuduClientBuilder("cdh02").build();
        KuduSession kuduSession = kuduClient.newSession();
        KuduTable kuduTable = kuduClient.openTable("impala::education.dwd_base_ad");
        Schema schema = kuduTable.getSchema();
        KuduPredicate eqpred = KuduPredicate.newComparisonPredicate(schema.getColumn("adid"), KuduPredicate.ComparisonOp.EQUAL, 1);
        KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable).addPredicate(eqpred).build();
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult next = rowResults.next();
                System.out.println(next.getString("adname"));
            }
        }
    }

//    public static void main(String[] args) throws Exception {
//
//
//        AsyncKuduClient kuduClient = new AsyncKuduClient.AsyncKuduClientBuilder("hadoop101").build();
//        AsyncKuduSession asyncKuduSession = kuduClient.newSession();
//        KuduTable kuduTable = kuduClient.openTable("impala::education.dwd_base_ad").join();
//        Schema schema = kuduTable.getSchema();
//        KuduPredicate eqpred = KuduPredicate.newComparisonPredicate(schema.getColumn("adid"), KuduPredicate.ComparisonOp.EQUAL, 1);
//        AsyncKuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).addPredicate(eqpred).build();
//        scanner.nextRows().addBoth(new Callback<String, RowResultIterator>() {
//            @Override
//            public String call(RowResultIterator arg) throws Exception {
//                String adname="";
//                 while (arg.hasNext()){
//                    adname=arg.next().getString("adname");
//                     System.out.println(adname);
//                 }
//                 return adname;
//            }
//        });
//        while (true){
//
//        }
//    }


    public static String generateHash(String input) {
        try {
            //参数校验
            if (null == input) {
                return null;
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            BigInteger bi = new BigInteger(1, digest);
            String hashText = bi.toString(16);
            while (hashText.length() < 32) {
                hashText = "0" + hashText;
            }
            return hashText;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
