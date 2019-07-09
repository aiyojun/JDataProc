package com.jpro;

public class SpcCon {
    private double normal;
    private double uplimit;
    private double downlimit;

    SpcCon(double nor, double up, double down) {
        normal = nor;
        uplimit = up;
        downlimit = down;
    }

    public double getNormal() {
        return normal;
    }

    public double getUplimit() {
        return uplimit;
    }

    public double getDownlimit() {
        return downlimit;
    }
}