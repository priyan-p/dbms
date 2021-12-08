package stream;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ConditionIterator<I, R> implements Iterator<Record, Boolean> {

    private List<Iterator<?, ?>> children;
    private Function<Record, Boolean> predicate;
    List<Record> records = new LinkedList<>();

    @Override
    public void setup(List<Iterator<?, ?>> children) {
        this.children = children;
    }

    @Override
    public void init(Function<Record, Boolean> function) {
        children.get(0).init((r) -> null);
        this.predicate = function;
    }

    @Override
    public boolean hasNext() {
        Iterator<?, ?> iterator = children.get(0);
        int c = 0;
        while (iterator.hasNext() && c < 2) {
            Record record = iterator.next();
            if (predicate.apply(record)) {
                records.add(record);
                c++;
            }
        }
        return !records.isEmpty();
    }

    @Override
    public Record next() {
        Record record = records.get(0);
        records.remove(0);
        return record;
    }

    @Override
    public void close() {
        for (Iterator<?, ?> iterator : children) {
            iterator.close();
        }
    }

}
