package com.ucas.design.factory02;

import com.ucas.design.factory01.Product;
import com.ucas.design.factory01.entity.Tv;

public class TvFactory implements Factory{
    public Product produce() {
        return new Tv();
    }
}
