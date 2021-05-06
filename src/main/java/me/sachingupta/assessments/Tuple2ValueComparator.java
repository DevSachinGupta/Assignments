package me.sachingupta.assessments;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class Tuple2ValueComparator implements Comparator<Tuple2<Long, Long>>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple2<Long, Long> tuple1, Tuple2<Long, Long> tuple2) {
		if (tuple2._2.compareTo(tuple1._2) == 0) {
            return tuple2._1.compareTo(tuple1._1);
        }
        return -tuple2._2.compareTo(tuple1._2);
	}

}
