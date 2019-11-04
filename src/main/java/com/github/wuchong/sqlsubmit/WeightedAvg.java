//package com.github.wuchong.sqlsubmit;
//
//import org.apache.flink.table.functions.AggregateFunction;
//
//import java.util.Iterator;
//
//public class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {
//
//    @Override
//    public WeightedAvgAccum createAccumulator() {
//        return new WeightedAvgAccum();
//    }
//
//    @Override
//    public Double getValue(WeightedAvgAccum acc) {
//        if (acc.count == 0) {
//            return null;
//        } else {
//            return acc.sum / acc.count;
//        }
//    }
//
//    @Override
//    public void accumulate(WeightedAvgAccum acc, double iValue) {
//        acc.sum += iValue * iWeight;
//        acc.count += iWeight;
//    }
//
//    @Override
//    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
//        acc.sum -= iValue * iWeight;
//        acc.count -= iWeight;
//    }
//
//    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
//        Iterator<WeightedAvgAccum> iter = it.iterator();
//        while (iter.hasNext()) {
//            WeightedAvgAccum a = iter.next();
//            acc.count += a.count;
//            acc.sum += a.sum;
//        }
//    }
//
//    public void resetAccumulator(WeightedAvgAccum acc) {
//        acc.count = 0;
//        acc.sum = 0L;
//    }
//}
