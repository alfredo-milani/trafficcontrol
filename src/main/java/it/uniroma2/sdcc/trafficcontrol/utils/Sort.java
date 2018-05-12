package it.uniroma2.sdcc.trafficcontrol.utils;

public class Sort {

    private double [] array;

    public Sort(double [] sort){
        this.array=sort;
    }

    public void bubbleSort() {

        for(int i = 0; i < this.array.length; i++) {
            boolean flag = false;
            for(int j = 0; j < this.array.length-1; j++) {

                //Se l' elemento j e maggiore del successivo allora
                //scambiamo i valori
                if(this.array[j]>this.array[j+1]) {
                    double k = this.array[j];
                    this.array[j] = this.array[j+1];
                    this.array[j+1] = k;
                    flag=true; //Lo setto a true per indicare che é avvenuto uno scambio
                }


            }

            if(!flag) break; //Se flag=false allora vuol dire che nell' ultima iterazione
            //non ci sono stati scambi, quindi il metodo può terminare
            //poiché l' array risulta ordinato
        }
    }

    public double[] getArray() {
        return array;
    }

    public void setArray(double[] array) {
        this.array = array;
    }
}
