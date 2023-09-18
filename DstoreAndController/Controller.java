import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Client thread.
 */
class ClientThread extends Thread {

    private Socket socket;
    private Controller controller;
    private BufferedReader input;
    private PrintWriter output;
    private HashMap<String, ArrayList<Integer> > reloadDstore;

    /**
     * Constructor.
     * 
     * @param socket
     * @param controller
     */
    public ClientThread(Socket socket, Controller controller) {
        this.socket = socket;
        this.controller = controller;
        this.reloadDstore = new HashMap<>();
    }

    
    /**
     * Run thread.
     */
    @Override
    public void run() {
        try {
            input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            output = new PrintWriter(socket.getOutputStream());
            while (true) {
                String command = input.readLine();
                if (command == null) {
                    input.close();
                    output.close();
                    return;
                }
                String token = command.split(" ")[0];
                System.out.println(command);
                if (token.equals(Protocol.LIST_TOKEN)) {
                    handleList();
                } else if (token.equals(Protocol.STORE_TOKEN)) {
                    handleStore(command);
                } else if (token.equals(Protocol.STORE_ACK_TOKEN)) {
                    handleStoreAck(command);
                } else if (token.equals(Protocol.LOAD_TOKEN)) {
                    handleLoad(command, false);
                } else if (token.equals(Protocol.RELOAD_TOKEN)) {
                    handleLoad(command, true);
                } else if (token.equals(Protocol.REMOVE_TOKEN)) {
                    handleRemove(command);
                } else if (token.equals(Protocol.JOIN_TOKEN)) {
                    handleJoin(command);
                } else if (token.equals(Protocol.REMOVE_ACK_TOKEN) || token.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) {
                    handleRemoveAck(command);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle command LIST.
     */
    private void handleList() {
        StringBuffer stringBuffer = new StringBuffer();
        if (!controller.hasEnoughDstoreMeta()) {
            stringBuffer.append(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            String filename = controller.getAllFilename();
            stringBuffer.append(Protocol.LIST_TOKEN);
            stringBuffer.append(" ");
            stringBuffer.append(filename);
        }
        output.println(stringBuffer.toString());
        output.flush();
    }

    /**
     * Handle command STORE.
     * 
     * @param command
     */
    private void handleStore(String command) {
        StringBuffer stringBuffer = new StringBuffer();
        String[] tokens = command.split(" ");
        if (tokens.length != 3) {
            return;
        }
        String filename = tokens[1];
        int filesize = Integer.valueOf(tokens[2]);
        if (filesize < 0) {
            return;
        }
        if (!controller.hasEnoughDstoreMeta()) {
            stringBuffer.append(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (controller.containsFile(filename)) {
            stringBuffer.append(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        } else {
            FileMeta fileMeta = new FileMeta(filename, filesize);
            fileMeta.setPrintWriter(output);
            if (controller.addFileMeta(fileMeta) == false) {
                stringBuffer.append(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            } else {
                stringBuffer.append(Protocol.STORE_TO_TOKEN);
                stringBuffer.append(" ");
                stringBuffer.append(controller.getDstoreMetaPorts());
            }
        }
        output.println(stringBuffer.toString());
        output.flush();
    }

    /**
     * Handle command STORE_ACK.
     */
    private void handleStoreAck(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        int port = socket.getPort();
        int dport = controller.getPortMap(port);
        if (dport != 0) {
            controller.setStoreFinished(dport, filename);
        }
    }

    /**
     * Handle command LOAD.
     */
    private void handleLoad(String command, boolean reload) {
        StringBuffer stringBuffer = new StringBuffer();
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        if (!controller.hasEnoughDstoreMeta()) {
            stringBuffer.append(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!controller.containsFile(filename)) {
            stringBuffer.append(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            FileMeta fileMeta = controller.getFileMeta(filename);
            if (fileMeta == null || fileMeta.getDStoreMeta().size() == 0) {
                stringBuffer.append(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                int port = fileMeta.getDStoreMeta().get(0).getPort();
                if (reload) {
                    port = -1;
                    for (int i = 1; i < fileMeta.getDStoreMeta().size(); i++) {
                        int tempPort = fileMeta.getDStoreMeta().get(i).getPort();
                        if (!reloadDstore.containsKey(filename)) {
                            reloadDstore.put(filename, new ArrayList<>());
                        }
                        if (!reloadDstore.get(filename).contains(tempPort)) {
                            port = tempPort;
                            reloadDstore.get(filename).add(tempPort);
                            break;
                        }
                    }
                }
                if (port != -1) {
                    stringBuffer.append(Protocol.LOAD_FROM_TOKEN);
                    stringBuffer.append(" ");
                    stringBuffer.append(port);
                    stringBuffer.append(" ");
                    stringBuffer.append(fileMeta.getFilesize());
                } else {
                    stringBuffer.append(Protocol.ERROR_LOAD_TOKEN);
                }
            }
        }
        output.println(stringBuffer.toString());
        output.flush();
    }

    /**
     * Handle command REMOVE.
     * 
     * @param command
     */
    private void handleRemove(String command) {
        StringBuffer stringBuffer = new StringBuffer();
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        if (!controller.hasEnoughDstoreMeta()) {
            stringBuffer.append(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!controller.containsFile(filename)) {
            stringBuffer.append(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            ArrayList<Integer> ports = controller.prepareRemoveFile(filename);
            if (ports == null) {
                stringBuffer.append(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            } else {
                for (Integer port: ports) {
                    try {
                        DstoreMeta dstoreMeta = controller.getDStoreMeta(port);
                        PrintWriter doutput = dstoreMeta.getOutput();
                        doutput.println(Protocol.REMOVE_TOKEN + " " + filename);
                        doutput.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                controller.finishedRemoveFile(filename);
                stringBuffer.append(Protocol.REMOVE_COMPLETE_TOKEN);
            }
        }
        output.println(stringBuffer.toString());
        output.flush();
    }

    /**
     * Handle command JOIN.
     * 
     * @param command
     */
    private void handleJoin(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        int port = Integer.valueOf(tokens[1]);
        DstoreMeta DstoreMeta = new DstoreMeta(port, input, output);
        controller.addDstoreMeta(DstoreMeta);
        controller.addPortMap(socket.getPort(), port);
        controller.rebalance();
        System.out.println("Port: " + port + " joined.");
    }

    /**
     * Handle command REMOVE_ACK.
     * 
     * @param command
     */
    private void handleRemoveAck(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        int port = socket.getPort();
        int dport = controller.getPortMap(port);
        if (dport != 0) {
            controller.removeFileFromDstore(port, filename);
        }
    }
}

/**
 * Period thread.
 */
class PeriodThread extends Thread {
    private int period;
    private Controller controller;

    /**
     * Constructor.
     * 
     * @param period
     * @param controller
     */
    public PeriodThread(int period, Controller controller) {
        this.period = period;
        this.controller = controller;
    }

    /**
     * Start thread.
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(period);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            controller.rebalance();
        }
    }
}

/**
 * File status.
 */
class Status {
    public final static String STORE_IN_PROGRESS = "store in progress";
    public final static String STORE_COMPLETE = "store complete";
    public final static String REMOVE_IN_PROGRESS = "remove in progress";
    public final static String REMOVE_COMPLETE = "remove complete";
};

/**
 * File meta.
 */
class FileMeta {
    private String filename;
    private int filesize;
    private String status;
    private PrintWriter printWriter;
    private ArrayList<DstoreMeta> dstoreMeta;

    /**
     * Constructor.
     * 
     * @param filename
     * @param filesize
     */
    public FileMeta(String filename, int filesize) {
        this.filename = filename;
        this.filesize = filesize;
        this.status = Status.STORE_IN_PROGRESS;
        this.printWriter = null;
        this.dstoreMeta = new ArrayList<>();
    }

    /**
     * Get filename.
     * 
     * @return
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Get filesize.
     * 
     * @return
     */
    public int getFilesize() {
        return filesize;
    }

    /**
     * Get file status.
     * 
     * @return
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set file status.
     * 
     * @param status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Add dstore meta.
     * 
     * @param dstoreMeta
     */
    public void addDStoreMeta(DstoreMeta dstoreMeta) {
        this.dstoreMeta.add(dstoreMeta);
    }

    /**
     * Remove dstore meta.
     * 
     * @param dstoreMeta
     */
    public void removeDstoreMeta(DstoreMeta dstoreMeta) {
        this.dstoreMeta.remove(dstoreMeta);
    }

    /**
     * Get dstore meta.
     * @return
     */
    public ArrayList<DstoreMeta> getDStoreMeta() {
        return dstoreMeta;
    }

    /**
     * Get r count.
     * 
     * @return
     */
    public int getRCount() {
        return dstoreMeta.size();
    }

    /**
     * Set print writer.
     * 
     * @param printWriter
     */
    public void setPrintWriter(PrintWriter printWriter) {
        this.printWriter = printWriter;
    }

    /**
     * Get print writer.
     * 
     * @return
     */
    public PrintWriter getPrintWriter() {
        return printWriter;
    }
}

/**
 * Dstore meta.
 */
class DstoreMeta implements Comparable<DstoreMeta> {
    private int port;
    private BufferedReader input;
    private PrintWriter output;
    private HashMap<String, FileMeta> fileMeta;

    /**
     * Constructor.
     * 
     * @param port
     * @param input
     * @param output
     */
    public DstoreMeta(int port, BufferedReader input, PrintWriter output) {
        this.port = port;
        this.input = input;
        this.output = output;
        this.fileMeta = new HashMap<>();
    }

    /**
     * Get port.
     * 
     * @return
     */
    public int getPort() {
        return port;
    }

    /**
     * Get input.
     * 
     * @return
     */
    public BufferedReader getInput() {
        return input;
    }

    /**
     * Get output.
     * 
     * @return
     */
    public PrintWriter getOutput() {
        return output;
    }

    /**
     * Contains file.
     * 
     * @param filename
     * @return
     */
    public boolean containsFile(String filename) {
        return fileMeta.containsKey(filename);
    }
    
    /**
     * Get file meta.
     * 
     * @param filename
     * @return
     */
    public FileMeta getFileMeta(String filename) {
        return fileMeta.get(filename);
    }

    /**
     * Add file meta.
     * 
     * @param fileMeta
     */
    public void addFileMeta(FileMeta fileMeta) {
        this.fileMeta.put(fileMeta.getFilename(), fileMeta);
    }

    /**
     * Remove file meta.
     * 
     * @param filename
     */
    public void removeFileMeta(String filename) {
        FileMeta fileMeta = this.fileMeta.get(filename);
        if (fileMeta != null) {
            fileMeta.removeDstoreMeta(this);
        }
        this.fileMeta.remove(filename);
    }

    /**
     * Get all filename.
     * 
     * @return
     */
    public ArrayList<String> getAllFilename() {
        ArrayList<String> filenames = new ArrayList<>();
        for (String filename: fileMeta.keySet()) {
            filenames.add(filename);
        }
        return filenames;
    }

    /**
     * Get file count.
     * 
     * @return
     */
    public int getFileCount() {
        return fileMeta.size();
    }

    @Override
    public int compareTo(DstoreMeta o) {
        return this.fileMeta.size() - o.fileMeta.size();
    }
};

/**
 * Class controller.
 */
public class Controller {

    private int cport;
    private int R;
    private int timeout;
    private int rebalancePeriod;

    private HashMap<String, FileMeta> fileMeta;
    private HashMap<Integer, DstoreMeta> dStoreMeta;
    private HashMap<Integer, Integer> portMap;
    private ReadWriteLock readWriteLock;

    /**
     * Constructor.
     * 
     * @param cport
     * @param R
     * @param timeout
     * @param rebalancePeriod
     */
    public Controller(int cport, int R, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.fileMeta = new HashMap<>();
        this.dStoreMeta = new HashMap<>();
        this.portMap = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();

        this.handlePeriod();
        this.handleRequest();
    }

    /**
     * Get timeout.
     * @return
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Add dstore meta.
     * 
     * @param dStoreMeta
     */
    public void addDstoreMeta(DstoreMeta dStoreMeta) {
        readWriteLock.writeLock().lock();
        this.dStoreMeta.put(dStoreMeta.getPort(), dStoreMeta);
        readWriteLock.writeLock().unlock();
    }

    /**
     * Get dstore meta count.
     * 
     * @return
     */
    public int getDstoreMetaCount() {
        int count = 0;
        readWriteLock.readLock().lock();
        count = this.dStoreMeta.size();
        readWriteLock.readLock().unlock();
        return count;
    }

    /**
     * Get dstore meta by port.
     * 
     * @param port
     * @return
     */
    public DstoreMeta getDStoreMeta(int port) {
        DstoreMeta dstoreMeta = null;
        readWriteLock.readLock().lock();
        dstoreMeta = this.dStoreMeta.get(port);
        readWriteLock.readLock().unlock();
        return dstoreMeta;
    }

    /**
     * Has enough dstore meta.
     * 
     * @return
     */
    public boolean hasEnoughDstoreMeta() {
        return getDstoreMetaCount() >= this.R;
    }

    /**
     * Get all filename.
     * 
     * @return
     */
    public String getAllFilename() {
        StringBuilder stringBuilder = new StringBuilder();
        readWriteLock.readLock().lock();
        for (FileMeta file: fileMeta.values()) {
            if (file.getStatus().equals(Status.STORE_COMPLETE)) {
                stringBuilder.append(file.getFilename());
                stringBuilder.append(" ");
            }
        }
        readWriteLock.readLock().unlock();
        return stringBuilder.toString().trim();
    }

    /**
     * Contains file.
     * 
     * @param filename
     * @return
     */
    public boolean containsFile(String filename) {
        boolean ret = false;
        readWriteLock.readLock().lock();
        if (this.fileMeta.containsKey(filename)) {
            ret = true;
        }
        readWriteLock.readLock().unlock();
        return ret;
    }

    /**
     * Add file meta.
     * 
     * @param fileMeta
     * @return
     */
    public boolean addFileMeta(FileMeta fileMeta) {
        boolean ret = true;
        readWriteLock.writeLock().lock();
        if (this.fileMeta.containsKey(fileMeta.getFilename())) {
            if (this.fileMeta.get(fileMeta.getFilename()).getStatus().equals(Status.REMOVE_COMPLETE)) {
                this.fileMeta.put(fileMeta.getFilename(), fileMeta);
            } else {
                ret = false;
            }
        } else {
            this.fileMeta.put(fileMeta.getFilename(), fileMeta);
        }
        readWriteLock.writeLock().unlock();
        return ret;
    }

    /**
     * Get file meta.
     * 
     * @param fileName
     * @return
     */
    public FileMeta getFileMeta(String fileName) {
        FileMeta fileMeta = null;
        readWriteLock.readLock().lock();
        fileMeta = this.fileMeta.get(fileName);
        readWriteLock.readLock().unlock();
        return fileMeta;
    }

    /**
     * Get dstore meta ports.
     * 
     * @return
     */
    public String getDstoreMetaPorts() {
        StringBuilder stringBuilder = new StringBuilder();
        readWriteLock.readLock().lock();
        ArrayList<DstoreMeta> DstoreMetaList = new ArrayList<>(dStoreMeta.values());
        Collections.sort(DstoreMetaList);
        for (int i = 0; i < R; i++) {
            stringBuilder.append(DstoreMetaList.get(i).getPort());
            stringBuilder.append(" ");
        }
        readWriteLock.readLock().unlock();
        return stringBuilder.toString().trim();
    }

    /**
     * Add port map.
     * 
     * @param port1
     * @param port2
     */
    public void addPortMap(int port1, int port2) {
        readWriteLock.writeLock().lock();
        portMap.put(port1, port2);
        readWriteLock.writeLock().unlock();
    }

    /**
     * Get port map.
     * 
     * @param port
     * @return
     */
    public int getPortMap(int port) {
        int p = 0;
        readWriteLock.readLock().lock();
        if (portMap.containsKey(port)) {
            p = portMap.get(port);
        }
        readWriteLock.readLock().unlock();
        return p;
    }

    /**
     * Set store finished.
     * 
     * @param port
     * @param filename
     */
    public void setStoreFinished(int port, String filename) {
        readWriteLock.writeLock().lock();
        DstoreMeta dstoreMeta = dStoreMeta.get(port);
        FileMeta fileMeta = this.fileMeta.get(filename);
        if (dstoreMeta != null && fileMeta != null) {
            fileMeta.addDStoreMeta(dstoreMeta);
            dstoreMeta.addFileMeta(fileMeta);
            if (fileMeta.getRCount() == R) {
                fileMeta.setStatus(Status.STORE_COMPLETE);
                System.out.println("File " + filename + " store complete.");
                PrintWriter printWriter = fileMeta.getPrintWriter();
                if (printWriter != null) {
                    printWriter.println(Protocol.STORE_COMPLETE_TOKEN);
                    printWriter.flush();
                }
            }
        }
        readWriteLock.writeLock().unlock();
    }

    /**
     * Prepare remove file.
     * 
     * @param filename
     * @return
     */
    public ArrayList<Integer> prepareRemoveFile(String filename) {
        ArrayList<Integer> ports = new ArrayList<>();
        readWriteLock.writeLock().lock();
        FileMeta fileMeta = this.fileMeta.get(filename);
        if (fileMeta != null) {
            if (fileMeta.getStatus().equals(Status.REMOVE_IN_PROGRESS)) {
                readWriteLock.writeLock().unlock();
                return null;
            }
            fileMeta.setStatus(Status.REMOVE_IN_PROGRESS);
            for (DstoreMeta dstoreMeta: fileMeta.getDStoreMeta()) {
                ports.add(dstoreMeta.getPort());
            }
        }
        readWriteLock.writeLock().unlock();
        return ports;
    }

    /**
     * Finished remove file.
     * 
     * @param filename
     */
    public void finishedRemoveFile(String filename) {
        readWriteLock.writeLock().lock();
        FileMeta fileMeta = this.fileMeta.get(filename);
        if (fileMeta != null) {
            fileMeta.setStatus(Status.REMOVE_COMPLETE);
            System.out.println("File " + filename + " remove complete.");
            this.fileMeta.remove(filename);
        }
        readWriteLock.writeLock().unlock();
    }

    /**
     * Remove file from dstore.
     * 
     * @param port
     * @param filename
     */
    public void removeFileFromDstore(int port, String filename) {
        readWriteLock.writeLock().lock();
        DstoreMeta dstoreMeta = this.dStoreMeta.get(port);
        if (dstoreMeta != null) {
            dstoreMeta.removeFileMeta(filename);
        }
        readWriteLock.writeLock().unlock();
    }

    /**
     * Rebalance.
     */
    public void rebalance() {
        readWriteLock.writeLock().lock();
        HashMap<DstoreMeta, ArrayList<String> > data = new HashMap<>();
        ArrayList<DstoreMeta> badDstore = new ArrayList<>();
        for (DstoreMeta dstoreMeta: this.dStoreMeta.values()) {
            try {
                data.put(dstoreMeta, new ArrayList<>());
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("127.0.0.1", dstoreMeta.getPort()), timeout);
                BufferedReader dinput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter doutput = new PrintWriter(socket.getOutputStream());
                doutput.println(Protocol.LIST_TOKEN);
                doutput.flush();
                String response = dinput.readLine();
                if (response.startsWith(Protocol.LIST_TOKEN)) {
                    String[] files = response.split(" ");
                    for (int i = 1; i < files.length; i++) {
                        data.get(dstoreMeta).add(files[i]);
                    }
                }
                dinput.close();
                doutput.close();
                socket.close();
            } catch (Exception e) {
                badDstore.add(dstoreMeta);
            }
        }

        // Remove bad Dstore.
        for (DstoreMeta dstoreMeta: badDstore) {
            this.dStoreMeta.remove(dstoreMeta.getPort());
        }

        // Make rebalance plan.
        HashMap<DstoreMeta, ArrayList<String> > plan = getCurrentStorageFiles();
        HashMap<String, Integer> fileReplecation = checkFileReplication(data);
        boolean change = true;
        while (change) {
            change = makeSendPlan(plan, fileReplecation);
            String filename = makeRemovePlan(plan);
            if (!filename.equals("")) {
                change = false;
                fileReplecation = new HashMap<>();
                fileReplecation.put(filename, 1);
            }
        }

        // Convert rebalance plan to command.
        HashMap<DstoreMeta, String> commandPlan = makeCommandPlan(plan);

        // Send plan.
        for (DstoreMeta dstoreMeta: commandPlan.keySet()) {
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("127.0.0.1", dstoreMeta.getPort()), timeout);
                BufferedReader dinput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter doutput = new PrintWriter(socket.getOutputStream());
                String command = commandPlan.get(dstoreMeta);
                doutput.println(command);
                doutput.flush();
                String response = dinput.readLine();
                if (response.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                    String[] parts = command.split(" ");
                    int num = 1;
                    int numberToSend = Integer.valueOf(parts[num++]);
                    for (int i = 0; i < numberToSend; i++) {
                        String filename = parts[num++];
                        int portToSend = Integer.valueOf(parts[num++]);
                        for (int j = 0; j < portToSend; j++) {
                            int dport = Integer.valueOf(parts[num++]);
                            this.dStoreMeta.get(dport).addFileMeta(this.fileMeta.get(filename));
                        }
                    }
                    int numberToRemove = Integer.valueOf(parts[num++]);
                    for (int i = 0; i < numberToRemove; i++) {
                        String filename = parts[num++];
                        dstoreMeta.removeFileMeta(filename);
                    }
                }
                dinput.close();
                doutput.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        readWriteLock.writeLock().unlock();
    }

    /**
     * Get current storage files.
     * 
     * @return
     */
    private HashMap<DstoreMeta, ArrayList<String> > getCurrentStorageFiles() {
        HashMap<DstoreMeta, ArrayList<String> > current = new HashMap<>();
        for (DstoreMeta dstoreMeta: this.dStoreMeta.values()) {
            current.put(dstoreMeta, dstoreMeta.getAllFilename());
        }
        return current;
    }

    /**
     * Check file replication.
     * 
     * @param data
     * @return
     */
    private HashMap<String, Integer> checkFileReplication(HashMap<DstoreMeta, ArrayList<String> > data) {
        HashMap<String, Integer> stat = new HashMap<>();
        for (ArrayList<String> filenames: data.values()) {
            for (String filename: filenames) {
                if (!stat.containsKey(filename)) {
                    stat.put(filename, 0);
                }
                stat.put(filename, stat.get(filename) + 1);
            }
        }
        HashMap<String, Integer> result = new HashMap<>();
        for (Entry<String, Integer> entry: stat.entrySet()) {
            if (entry.getValue() < R) {
                result.put(entry.getKey(), R - entry.getValue());
            }
        }
        return result;
    }

    /**
     * Make sending plan.
     * 
     * @param plan
     * @param fileReplecation
     * @return
     */
    private boolean makeSendPlan(HashMap<DstoreMeta, ArrayList<String> > plan, HashMap<String, Integer> fileReplecation) {
        boolean change = false;
        for (Entry<String, Integer> entry: fileReplecation.entrySet()) {
            String filename = entry.getKey();
            int newCount = entry.getValue();
            for (int count = 0; count < entry.getValue(); count++) {
                DstoreMeta best = null;
                for (DstoreMeta dstoreMeta: plan.keySet()) {
                    if (!dstoreMeta.containsFile(filename)) {
                        if (best == null) {
                            best = dstoreMeta;
                        } else if (dstoreMeta.getFileCount() < best.getFileCount()) {
                            best = dstoreMeta;
                        }
                    }
                }
                if (best != null) {
                    best.addFileMeta(new FileMeta(filename, -1));
                    newCount--;
                    change = true;
                }
            }
            fileReplecation.put(filename, newCount);
        }
        return change;
    }

    /**
     * Make remove plan.
     * 
     * @param plan
     * @return
     */
    private String makeRemovePlan(HashMap<DstoreMeta, ArrayList<String> > plan) {
        String filename = "";
        for (Entry<DstoreMeta, ArrayList<String>> entry: plan.entrySet()) {
            int upper = R * fileMeta.size() / plan.size();
            if (entry.getValue().size() > upper) {
                Random random = new Random();
                int index = random.nextInt(entry.getValue().size());
                filename = entry.getValue().get(index);
                entry.getValue().remove(index);
                return filename;
            } 
        }
        return filename;
    }

    /**
     * Make command plan.
     * 
     * @param plan
     * @return
     */
    private HashMap<DstoreMeta, String> makeCommandPlan(HashMap<DstoreMeta, ArrayList<String> > plan) {
        // Sending plan.
        HashMap<DstoreMeta, HashMap<String, String> > fileToSend = new HashMap<>();
        for (DstoreMeta dstoreMeta: plan.keySet()) {
            fileToSend.put(dstoreMeta, new HashMap<>());
        }
        for (DstoreMeta dstoreMeta: plan.keySet()) {
            for (String filename: dstoreMeta.getAllFilename()) {
                FileMeta fileMeta = dstoreMeta.getFileMeta(filename);
                if (fileMeta.getFilesize() == -1) {
                    for (DstoreMeta dstoreMeta1: plan.keySet()) {
                        if (!dstoreMeta1.equals(dstoreMeta) && dstoreMeta1.containsFile(filename)) {
                            HashMap<String, String> ports = fileToSend.get(dstoreMeta1);
                            if (!ports.containsKey(filename)) {
                                ports.put(filename, "");
                            }
                            ports.put(filename, ports.get(filename) + " " + dstoreMeta.getPort());
                            break;
                        }
                    }
                }
            }
        }
        // Remove plan.
        HashMap<DstoreMeta, ArrayList<String> > fileToRemove = new HashMap<>();
        for (DstoreMeta dstoreMeta: plan.keySet()) {
            fileToRemove.put(dstoreMeta, new ArrayList<>());
        }
        for (DstoreMeta dstoreMeta: this.dStoreMeta.values()) {
            for (String filename: dstoreMeta.getAllFilename()) {
                if (!plan.get(dstoreMeta).contains(filename)) {
                    fileToRemove.get(dstoreMeta).add(filename);
                }
            }
        }
        HashMap<DstoreMeta, String> commands = new HashMap<>();
        for (DstoreMeta dstoreMeta: plan.keySet()) {
            StringBuilder stringBuilder = new StringBuilder();
            HashMap<String, String> toSend = fileToSend.get(dstoreMeta);
            stringBuilder.append(toSend.size());
            stringBuilder.append(" ");
            for (Entry<String, String> entry: toSend.entrySet()) {
                String filename = entry.getKey();
                stringBuilder.append(filename);
                stringBuilder.append(" ");
                int count = entry.getValue().trim().split(" ").length;
                stringBuilder.append(count);
                stringBuilder.append(" ");
                stringBuilder.append(entry.getValue().trim());
                stringBuilder.append(" ");
            }
            ArrayList<String> toRemove = fileToRemove.get(dstoreMeta);
            stringBuilder.append(toRemove.size());
            for (String filename: toRemove) {
                stringBuilder.append(" ");
                stringBuilder.append(filename);
            }
            String command = stringBuilder.toString();
            command.replace("  ", " ");
            if (!command.equals("0 0")) {
                commands.put(dstoreMeta, Protocol.REBALANCE_TOKEN + " " + command);
            }
        }
        return commands;
    }

    /**
     * Handle request.
     */
    private void handleRequest() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(cport);
            System.out.println("Controller server listen on port: " + cport);
            while (true) {
                Socket socket = serverSocket.accept();
                ClientThread clientThread = new ClientThread(socket, this);
                clientThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle period thread.
     */
    private void handlePeriod() {
        PeriodThread periodThread = new PeriodThread(rebalancePeriod, this);
        periodThread.start();
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Controller <cport> <R> <timeout> <rebalance_period>");
            return;
        }
        int cport = Integer.valueOf(args[0]);
        int R = Integer.valueOf(args[1]);
        int timeout = Integer.valueOf(args[2]);
        int rebalancePeriod = Integer.valueOf(args[3]);

        new Controller(cport, R, timeout, rebalancePeriod);
    }
}