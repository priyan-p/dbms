package stream;

public class Record {

    private int data;

    public Record(int data) {
        this.data = data;
    }

    public int getData() {
        return data;
    }

    public void setData(int data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Record [data=" + data + "]";
    }

}
