package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WdSortComparator extends WritableComparator {

    public WdSortComparator() {
        super(WdKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 年、月 温度倒序
        WdKey k1 = (WdKey) a;
        WdKey k2 = (WdKey) b;

        int c = Integer.compare(k1.getYear(), k2.getYear());
        if (c != 0) {
            return c;
        }

        c = Integer.compare(k1.getMonth(), k2.getMonth());
        if (c != 0) {
            return c;
        }

        return -Integer.compare(k1.getWd(), k2.getWd());
    }
}
