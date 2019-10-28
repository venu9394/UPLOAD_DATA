package com.hhcl.upload;

class PrepareData {
    public int value;
    public String name;
    public String Type;
    public PrepareData(int value, String name,String Type) {
	this.value = value;
	this.name = name;
	this.Type = Type;
    }
    public String toString() {
	//return "value = " + this.value + ", name = " + this.name;
     return  this.value+","+this.name+","+this.Type;
    }
}