package com.ucas.design.factory02;

import com.ucas.design.factory01.Product;
import com.ucas.design.factory01.entity.Car;

public class CarFactory implements Factory {
    public Product produce() {
        return new Car();
    }
}
