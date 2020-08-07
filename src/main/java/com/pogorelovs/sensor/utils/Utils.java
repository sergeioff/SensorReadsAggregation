package com.pogorelovs.sensor.utils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Collection;

public class Utils {
    public static <T> Seq<T> seq(Collection<T> collection) {
        return JavaConverters.collectionAsScalaIterableConverter(collection).asScala().toSeq();
    }
}
