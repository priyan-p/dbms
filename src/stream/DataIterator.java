package stream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DataIterator<I, R> implements Iterator<Integer, Integer> {

    private static List<Record> records = new ArrayList<>();
    private int currIdx = 0;
    boolean closed = false;

    @Override
    public void setup(List<Iterator<?, ?>> children) {
        for (int i = 0; i < 10; i++) {
            records.add(new Record(i));
        }
    }

    @Override
    public void init(Function<Integer, Integer> function) {
        Integer idx = function.apply(null);
        if (idx != null) {
            currIdx = idx;
        }
    }

    @Override
    public Record next() {
        if (closed)
            throw new RuntimeException("Iterator closed");
        return records.get(currIdx++);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean hasNext() {
        return currIdx < records.size();
    }

}
