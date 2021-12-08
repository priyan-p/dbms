package stream;

import java.util.List;

public class StreamTest {

    public static void main(String[] args) {

        System.out.println("Data Iterator");
        DataIterator<Integer, Integer> dataIterator = new DataIterator<>();
        dataIterator.setup(null);
        dataIterator.init((i) -> 2);
        while (dataIterator.hasNext()) {
            System.out.println(dataIterator.next());
        }

        System.out.println("Conditional Iterator");
        ConditionIterator<?, ?> conditionIterator = new ConditionIterator<>();
        conditionIterator.setup(List.of(new DataIterator<>()));
        conditionIterator.init((r) -> r.getData() % 2 == 0);
        while (conditionIterator.hasNext()) {
            System.out.println(conditionIterator.next());
        }
    }

}
