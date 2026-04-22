package com.membership;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
    //总共需要迁移的数据量
    public static final long maxId = 15000000L;
    //记录当前MongoDB数据库总共写入成功的数据
    public static long idx = 0L;
    //优先阻塞队列,按照批次头部id排序
    public static PriorityBlockingQueue<List<Long>> queue = new PriorityBlockingQueue<>(750,
            Comparator.comparing(batch -> batch.get(0)));
    public static int failCount = 0;

    public static void main(String[] args) {
        // write your code here
        Thread thread1 = new Thread(new BusinessTask1(), "读取1");
        Thread thread2 = new Thread(new BusinessTask2(), "读取2");
        Thread thread3 = new Thread(new BusinessTask3(), "读取3");
        Thread thread4 = new Thread(new InsertTask1(), "写入1");

        // 启动3个读取线程
        thread1.start();
        thread2.start();
        thread3.start();
        // 启动写入数据线程
        thread4.start();

    }

    static class BusinessTask1 implements Runnable {
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            long begin = 1L;
            handleData(begin);
            System.out.println(Thread.currentThread().getName() + "所花时间：" + (System.currentTimeMillis() - start) + "毫秒");
        }
    }

    static class BusinessTask2 implements Runnable {
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            long begin = 20001L;
            //读取所有数据
            handleData(begin);
            System.out.println(Thread.currentThread().getName() + "所花时间：" + (System.currentTimeMillis() - start) + "毫秒");
        }
    }

    static class BusinessTask3 implements Runnable {
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            long begin = 40001L;
            handleData(begin);
            System.out.println(Thread.currentThread().getName() + "所花时间：" + (System.currentTimeMillis() - start) + "毫秒");
        }
    }

    static class InsertTask1 implements Runnable {
        @Override
        public void run() {
            try {
                insertTaskMethod();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private static void insertTaskMethod() throws InterruptedException {
            long start = System.currentTimeMillis();
            while (idx <= maxId - 1) {
                // 阻塞取数据，没数据就会休眠，不消耗CPU
                List<Long> batch = queue.poll(100, TimeUnit.MILLISECONDS);
                if (batch != null && batch.get(0) != idx + 1) {
                    queue.put(batch);
                    continue;
                }
                // 插入MongoDB
                while (batch != null && batch.size() != 0) {
                    batch = insertToMongoDB(batch);
                }
                //记录总共插入的数据条数
                idx += 20000;
                System.out.println("插入成功！当前数据库已有：" + idx + "条数据。");
            }
            System.out.println(Thread.currentThread().getName() + " 总耗时：" + (System.currentTimeMillis() - start)
                    + "毫秒累计网络抖动" + failCount + "次");
        }
    }

    //模拟查询并处理数据
    public static void handleData(Long begin) {
        for (long i = begin; i < maxId; i += 60000) {
            //每轮查询20000条数据，查完sleep 3.6秒,这里默认id自增为1
            List<Long> selectList = selectFromMySQL(i, 20000L);
            try {
                //模拟数据库读取和预处理清洗数据的总时间，设置为测试值/100，便于测试
                Thread.sleep(36);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            queue.add(selectList);
        }
    }

    //模拟数据库游标分页查询
    public static List<Long> selectFromMySQL(Long id, Long limit) {
        List<Long> list = new ArrayList<>();
        for (long i = id; i < id + limit; i++) {
            list.add(i);
        }
        return list;
    }

    //模拟mongodb的写入
    public static List<Long> insertToMongoDB(List<Long> batchIds) throws InterruptedException {
        //动态指数概率模型模拟网络波动概率
        double failRate = 0.05 * Math.pow(batchIds.size() / 40000.0, 1.5);
        //粗略估算插入时间,写入时间指定为2万条数据的测试值/100，便于测试
        Thread.sleep(13);
        if (Math.random() < failRate) {
            int failIdx = (int) (Math.random() * batchIds.size());
            //单线程写入，所以直接用int累计失败次数
            failCount++;
            //递归写入
            return batchIds.subList(failIdx, batchIds.size());
        } else {
            return new ArrayList<>();
        }
    }
}
