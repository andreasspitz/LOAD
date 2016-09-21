package externalsort;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Parallel worker thread for sorting a list of intermediate LOAD edges in string format
 * 
 * Originally published July 2016 at
 * http://dbs.ifi.uni-heidelberg.de/?id=load
 * (c) 2016 Andreas Spitz (spitz@informatik.uni-heidelberg.de)
 */
public class SortingThreadWorker implements Runnable {
    List<String> list;
    Comparator<String> stringComparator;
    CountDownLatch latch;
    
    public SortingThreadWorker(List<String> list, Comparator<String> stringComparator, CountDownLatch latch) {
        this.list = list;
        this.stringComparator = stringComparator;
        this.latch = latch;
    }
         
    @Override
    public void run() {
        Collections.sort(list, stringComparator);
        latch.countDown();
    }
}