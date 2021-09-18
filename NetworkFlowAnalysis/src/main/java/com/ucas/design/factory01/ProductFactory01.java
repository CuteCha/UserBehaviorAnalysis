package com.ucas.design.factory01;

import com.ucas.design.factory01.entity.Car;
import com.ucas.design.factory01.entity.Tv;

public class ProductFactory01 {
    public static Product produce(String productName) throws Exception {
        if (productName.equals("tv")) {
            return new Tv();
        } else if (productName.equals("car")) {
            return new Car();
        } else {
            throw new Exception("没有该产品");
        }
    }
}
