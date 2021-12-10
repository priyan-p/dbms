package partitionsort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Scanner;

public class RoundRobinPartitioner implements Partitioner {

    @Override
    public void divide(InputStream source, List<OutputStream> destinations, String delimiter) {
        try (Scanner scanner = new Scanner(new BufferedInputStream(source))) {
            scanner.useDelimiter(delimiter);
            int i = 0;
            while (scanner.hasNext()) {
                int val = scanner.nextInt(), partitionKey = i * destinations.size();
                destinations.get(partitionKey).write(Integer.toString(val).getBytes());
                destinations.get(partitionKey).write(delimiter.getBytes());
                i++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void merge(List<InputStream> sources, OutputStream destination, String delimiter) {
        try (BufferedOutputStream out = new BufferedOutputStream(destination)) {
            PriorityQueue<Record> pq = createQFromPartitionFiles(sources, delimiter);
            while (!pq.isEmpty()) {
                Record record = pq.poll();
                out.write(String.valueOf(record.val).getBytes());
                if (record.scanner.hasNext())
                    pq.add(new Record(record.scanner.nextInt(), record.scanner));
                else
                    record.scanner.close();
                if (!pq.isEmpty())
                    out.write(delimiter.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private PriorityQueue<Record> createQFromPartitionFiles(List<InputStream> sources, String delimiter) {
        return sources.stream()
                .map(f -> {
                    Scanner scanner = getScanner(f, delimiter);
                    if (scanner != null && scanner.hasNext())
                        return new Record(scanner.nextInt(), scanner);
                    return null;
                })
                .filter((r) -> !Objects.isNull(r))
                .reduce(new PriorityQueue<Record>((r1, r2) -> r1.val - r2.val),
                        (pq, r) -> {
                            pq.add(r);
                            return pq;
                        }, (q1, q2) -> {
                            q1.addAll(q2);
                            return q1;
                        });
    }

    private Scanner getScanner(InputStream stream, String delimiter) {
        Scanner scanner = new Scanner(stream);
        scanner.useDelimiter(delimiter);
        return scanner;
    }

    class Record {
        private int val;
        private Scanner scanner;

        public Record(int val, Scanner scanner) {
            this.val = val;
            this.scanner = scanner;
        }
    }
}
