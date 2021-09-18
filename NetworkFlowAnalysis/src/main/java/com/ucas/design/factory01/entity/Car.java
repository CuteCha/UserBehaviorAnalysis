package com.ucas.design.factory01.entity;

import com.ucas.design.factory01.Product;

public class Car implements Product {
    public Car() {
        System.out.println("汽车被制造了");
    }
}
