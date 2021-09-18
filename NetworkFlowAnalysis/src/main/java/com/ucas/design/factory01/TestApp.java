package com.ucas.design.factory01;

import org.junit.Test;

public class TestApp {
    @Test
    public void test01() {
        try {
            ProductFactory01.produce("car");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test02() {
        try {
            ProductFactory02.produce("com.ucas.design.factory01.entity.Tv");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }
}
