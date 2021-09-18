package com.ucas.design.factory01.entity;

import com.ucas.design.factory01.Product;

public class Tv implements Product {
    public Tv(){
        System.out.println("电视被制造了");
    }
}
