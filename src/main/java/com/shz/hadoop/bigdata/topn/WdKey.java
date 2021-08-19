package com.shz.hadoop.bigdata.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WdKey implements WritableComparable<WdKey> {
    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    public int compareTo(WdKey o) {
        int c = Integer.compare(year, o.getYear());
        if (c != 0) {
            return c;
        }

        c = Integer.compare(month, o.getMonth());
        if (c != 0) {
            return c;
        }

        return Integer.compare(day, o.getDay());
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        month = in.readInt();
        day = in.readInt();
        wd = in.readInt();
    }
}
