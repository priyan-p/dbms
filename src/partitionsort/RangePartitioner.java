package partitionsort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class RangePartitioner implements Partitioner {

    private int maxValue;

    public RangePartitioner(int maxValue) {
        this.maxValue = maxValue;
    }

    @Override
    public void divide(InputStream source, List<OutputStream> destinations, String delimiter) {
        try (Scanner scanner = new Scanner(new BufferedInputStream(source))) {
            scanner.useDelimiter(delimiter);
            while (scanner.hasNext()) {
                int val = scanner.nextInt(), partitionKey = val / (maxValue / destinations.size());
                destinations.get(partitionKey).write(Integer.toString(val).getBytes());
                destinations.get(partitionKey).write(delimiter.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void merge(List<InputStream> sources, OutputStream destination, String delimiter) {
        try {
            List<InputStream> streams = new ArrayList<>();
            for (InputStream is : sources) {
                streams.add(is);
                streams.add(new ByteArrayInputStream(delimiter.getBytes()));
            }
            streams.remove(streams.size() - 1);
            SequenceInputStream sequenceInputStream = new SequenceInputStream(Collections.enumeration(streams));
            BufferedOutputStream bout = new BufferedOutputStream(destination);
            sequenceInputStream.transferTo(bout);
            bout.close();
            sequenceInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}
