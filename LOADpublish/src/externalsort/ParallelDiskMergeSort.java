package externalsort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Sorts a file externally on disk by using merge-sort
 * 
 * Based on the external memory sorting java implementation originally written by:
 * (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 * Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Sugumaran Harikrishnan,
 * Amit Jain, Thomas Mueller, Andreas Spitz, Jerry Yang.
 * First published: April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java/
 */
public class ParallelDiskMergeSort {
    
    private long totalLinesInFile;
    private int maxLinesPerTmpFile;
    private int writeBufferSize;
    private int nThreads;
    private Comparator<String> stringComparator;
    
    Comparator<FileBuffer> fileComparator = new Comparator<FileBuffer>() {
        @Override
        public int compare(FileBuffer i, FileBuffer j) {
            return stringComparator.compare(i.peek(), j.peek());
        }
    };
    
    Comparator<LinkedList<String>> listComparator = new Comparator<LinkedList<String>>() {
        @Override
        public int compare(LinkedList<String> i, LinkedList<String> j) {
            return stringComparator.compare(i.peek(), j.peek());
        }
    };
    
    public ParallelDiskMergeSort(long totalLines, int maxLinesPerTmpFile, int writeBufferSize,
                                 int nThreads, Comparator<String> stringComparator) {
        this.totalLinesInFile = totalLines;
        this.maxLinesPerTmpFile = maxLinesPerTmpFile;
        this.writeBufferSize = writeBufferSize;
        this.nThreads = nThreads;
        this.stringComparator = stringComparator;
    }
    
    /**
     * This merges a bunch of temporary flat files
     * 
     * @param files       The {@link List} of sorted {@link File}s to be merged.
     * @param outputfile  The output {@link File} to merge the results to.
     * @throws            IOException
     */
    private void mergeSortedFiles(List<File> files, File outputfile) throws IOException {
        
        // create priority queue for keeping files sorted by their top most string
        PriorityQueue<FileBuffer> pq = new PriorityQueue<FileBuffer>(11, fileComparator);
        
        // add temporary input file buffers to priority queue
        for (File f : files) {
            InputStream in = new FileInputStream(f);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            FileBuffer bfb = new FileBuffer(br);
            pq.add(bfb);
        }
        
        // create output file buffer
        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile), "UTF-8"), writeBufferSize);
        
        // while there are still files that contain strings (lines), pop the lines and add them to output file
        try {
            long nextpercent = totalLinesInFile / 100;
            int percentcount = 0;
            long currentLine = 0;
            
              while (pq.size() > 0) {
                   FileBuffer bfb = pq.poll();
                String r = bfb.pop();
                fbw.write(r);
                fbw.newLine();
                
                if (++currentLine == nextpercent) {
                    nextpercent += totalLinesInFile / 100;
                    percentcount++;
                    System.out.print("\rMerged " + percentcount + "% of lines.      ");
                }
                
                if (bfb.empty()) {
                     bfb.fbr.close();
                } else {
                      pq.add(bfb);
                }
            }
        } finally {
               fbw.close();
            for (FileBuffer bfb : pq) {
                  bfb.close();
            }
            System.out.println();
        }
        
        // clean up temporary files
        for (File f : files) {
            f.delete();
        }
    }

    /**
     * Sort a list and save it to a temporary file
     * 
     * @return the file containing the sorted data
     * @param tmplist      data to be sorted
     * @param tmpdirectory location of the temporary files (set to null for
     *                     default location)
     * @throws IOException
     */
    private File sortAndSave(List<LinkedList<String>> tmplists, File tmpdirectory) throws IOException {
        
        CountDownLatch latch = new CountDownLatch(nThreads);
        
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        for (int i=0; i<nThreads; i++) {
            SortingThreadWorker w = new SortingThreadWorker(tmplists.get(i), stringComparator, latch);
            executor.execute(w);
        }
        executor.shutdown();
        
        // wait for termination
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                System.out.println();
                System.out.println("Waiting was interrupted (sorting)");
            }
        }
        
        // create a new temporary file and set it to delete itself after use
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", tmpdirectory);
        newtmpfile.deleteOnExit();
        
        // create a buffered writer for the file
        OutputStream out = new FileOutputStream(newtmpfile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"), writeBufferSize);
        
        // create priority queue for keeping lists sorted by their top most string
        PriorityQueue<LinkedList<String>> pq = new PriorityQueue<LinkedList<String>>(11, listComparator);

        // add temporary input file buffers to priority queue
        for (LinkedList<String> l : tmplists) {
            if (!l.isEmpty()) {
                pq.add(l);
            }
        }

        // while there are still lists that contain strings (lines), pop the lines and add them to output file
        try {
              while (pq.size() > 0) {
                  LinkedList<String> l = pq.poll();
                String r = l.pop();
                bw.write(r);
                bw.newLine();
                
                if (!l.isEmpty()) {
                      pq.add(l);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
               bw.close();
        }
        
        return newtmpfile;
    }

    /**
     * @param fbr            data source
     * @param tmpdirectory    location of the temporary files (set to null for
     *                        default location)
     * @return a list of temporary flat files
     * @throws IOException
     */
    private List<File> sortInBatch(final BufferedReader fbr, final File tmpdirectory) throws IOException {
        
        // compute number of required temporary files
        int nFiles = (int) Math.ceil((double) totalLinesInFile / (double) maxLinesPerTmpFile);
        
        // create a list for temporary files for splitting the main file
        List<File> files = new ArrayList<File>();

        // read the file, split it into temporary files and sort them
        try {
            List<LinkedList<String>> tmplists = new ArrayList<LinkedList<String>>(nThreads);
            for (int i=0; i<nThreads; i++) {
                tmplists.add(new LinkedList<String>());
            }
            
            String line = "";
            int currentfile = 0;
            try {
                while (line != null) {
                    
                    currentfile++;
                    System.out.print("\rWorking on temporary file " + currentfile + "/" + nFiles + " (reading)     ");    
                    
                    int lineCount = 0;
                    while ((lineCount < maxLinesPerTmpFile) && ((line = fbr.readLine()) != null)) {
                        tmplists.get(lineCount % nThreads).add(line);
                        lineCount++;
                    }
                    
                    System.out.print("\rWorking on temporary file " + currentfile + "/" + nFiles + " (sorting)     ");
                    
                    files.add(sortAndSave(tmplists, tmpdirectory));
                    for (int i=0; i<nThreads; i++) {
                        tmplists.get(i).clear();
                    }
                }
            } catch (EOFException oef) {
                if (tmplists.get(0).size() > 0) {
                    System.out.print("\rWorking on temporary file " + currentfile + "/" + nFiles + " (sorting)     ");
                    files.add(sortAndSave(tmplists, tmpdirectory));
                }
            }
        } finally {
            fbr.close();
            System.out.println();
        }
        return files;
    }
    
    public void sortFile(File inputFile, File outputFile, File tempFileDir) {
        try {
            
            // read input file and sort into smaller files
            BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
            List<File> files = sortInBatch(bf, tempFileDir);
            
            // merge small sorted files into big sorted file
            mergeSortedFiles(files, outputFile);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
