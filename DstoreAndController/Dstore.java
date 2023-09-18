import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Constroller thread.
 */
class ControllerThread extends Thread {

    private Dstore dstore;
    private Socket socket;
    private BufferedReader input;
    private PrintWriter output;
    private InputStream in;
    private FileContent fileContent;

    /**
     * Constructor.
     * 
     * @param dstore
     */
    public ControllerThread(Dstore dstore) {
        this.dstore = dstore;
    }

    @Override
    public void run() {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", dstore.getCport()), dstore.getTimeout());
            in = socket.getInputStream();
            input = new BufferedReader(new InputStreamReader(in));
            output = new PrintWriter(socket.getOutputStream());
            dstore.setControllerInput(input);
            dstore.setControllerOutput(output);
            output.println(Protocol.JOIN_TOKEN + " " + dstore.getPort());
            output.flush();
            while (true) {
                String command = input.readLine();
                if (command == null) {
                    System.exit(0);
                }
                System.out.println("Recv from controller: " + command);
                String token = command.split(" ")[0];
                if (token.equals(Protocol.STORE_TOKEN) || token.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                    handleStore(command);
                } else if (token.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                    handleRebalanceStore(command);
                } else if (token.equals(Protocol.LOAD_DATA_TOKEN)) {
                    handleDataLoad(command);
                } else if (token.equals(Protocol.REMOVE_TOKEN)) {
                    handleRemove(command);
                } else if (token.equals(Protocol.LIST_TOKEN)) {
                    handleList(command);
                } else if (token.equals(Protocol.REBALANCE_TOKEN)) {
                    handleRebalance(command);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle command STORE.
     * 
     * @param command
     */
    private void handleStore(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 3) {
            return;
        }
        String filename = tokens[1];
        int filesize = Integer.valueOf(tokens[2]);
        if (filesize < 0) {
            return;
        }
        fileContent = new FileContent(filename, filesize, dstore.getFileFolder());
        dstore.addFile(fileContent);
        output.println(Protocol.ACK_TOKEN);
        output.flush();

        fileContent.writeContent(in);

        PrintWriter controllerOutput = dstore.getControllerOutput();
        controllerOutput.println(Protocol.STORE_ACK_TOKEN + " " + fileContent.getfilename());
        controllerOutput.flush();
    }

    private void handleRebalanceStore(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 3) {
            return;
        }
        String filename = tokens[1];
        int filesize = Integer.valueOf(tokens[2]);
        if (filesize < 0) {
            return;
        }
        fileContent = new FileContent(filename, filesize, dstore.getFileFolder());
        dstore.addFile(fileContent);
        output.println(Protocol.ACK_TOKEN);
        output.flush();

        fileContent.writeContent(in);
    }

    /**
     * Handle command LOAD_DATA
     * @param command
     */
    private void handleDataLoad(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        String path = dstore.getFileFolder() + File.separator + filename;
        try {
            FileInputStream fis = new FileInputStream(new File(path));
            OutputStream os = socket.getOutputStream();
            os.write(fis.readNBytes(this.dstore.getFilesize(filename)));
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle command REMOVE.
     * 
     * @param command
     */
    private void handleRemove(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        String path = dstore.getFileFolder() + File.separator + filename;
        File file = new File(path);
        file.delete();
        if (dstore.removeFile(filename)) {
            System.out.println("Send to controller: " + Protocol.REMOVE_ACK_TOKEN + " " + filename);
            output.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        } else {
            output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
        }
        output.flush();
    }

    /**
     * Handle command LIST.
     * 
     * @param command
     */
    private void handleList(String command) {
        String filenames = dstore.listFile();
        output.println(Protocol.LIST_TOKEN + " " + filenames);
        output.flush();
    }

    /**
     * Handle command REBALANCE.
     * 
     * @param command
     */
    private void handleRebalance(String command) {
        String[] parts = command.split(" ");
        int num = 1;
        int numberToSend = Integer.valueOf(parts[num++]);
        for (int i = 0; i < numberToSend; i++) {
            String filename = parts[num++];
            int portToSend = Integer.valueOf(parts[num++]);
            for (int j = 0; j < portToSend; j++) {
                int dport = Integer.valueOf(parts[num++]);
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("127.0.0.1", dport), this.dstore.getTimeout());
                    BufferedReader dinput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter doutput = new PrintWriter(socket.getOutputStream());
                    doutput.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + this.dstore.getFilesize(filename));
                    doutput.flush();
                    String response = dinput.readLine();
                    if (response.startsWith(Protocol.ACK_TOKEN)) {
                        String path = dstore.getFileFolder() + File.separator + filename;
                        
                        FileInputStream fis = new FileInputStream(new File(path));
                        OutputStream os = socket.getOutputStream();
                        os.write(fis.readNBytes(this.dstore.getFilesize(filename)));
                        os.flush();
                    }
                    dinput.close();
                    doutput.close();
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        int numberToRemove = Integer.valueOf(parts[num++]);
        for (int i = 0; i < numberToRemove; i++) {
            String filename = parts[num++];
            this.dstore.removeFile(filename);
        }
        output.println(Protocol.REBALANCE_COMPLETE_TOKEN);
        output.flush();
    }
}

/**
 * Class file content.
 */
class FileContent {
    private String filename;
    private int filesize;
    private FileOutputStream fileOutputStream;

    /**
     * Constructor.
     * 
     * @param filename
     * @param filesize
     * @param fileFolder
     */
    public FileContent(String filename, int filesize, String fileFolder) {
        this.filename = filename;
        this.filesize = filesize;
        try {
            this.fileOutputStream = new FileOutputStream(new File(fileFolder + File.separator + filename));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get filename.
     * 
     * @return
     */
    public String getfilename() {
        return filename;
    }

    /**
     * Get file size.
     * 
     * @return
     */
    public int getFilesize() {
        return filesize;
    }

    /**
     * Write file content.
     * 
     * @param content
     */
    public void writeContent(InputStream inputStream) {
        try {
            fileOutputStream.write(inputStream.readNBytes(filesize));
            fileOutputStream.flush();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

/**
 * Store thread.
 */
class StoreThread extends Thread {

    private Socket socket;
    private Dstore dstore;
    private InputStream in;
    private BufferedReader input;
    private PrintWriter output;
    private FileContent fileContent;

    /**
     * Constructor.
     * 
     * @param socket
     * @param dstore
     */
    public StoreThread(Socket socket, Dstore dstore) {
        this.socket = socket;
        this.dstore = dstore;
        this.fileContent = null;
    }

    @Override
    public void run() {
        try {
            in = socket.getInputStream();
            input = new BufferedReader(new InputStreamReader(in));
            output = new PrintWriter(socket.getOutputStream());
            while (true) {
                if (input == null) {
                    return;
                }
                String command = input.readLine();
                if (command == null) {
                    input.close();
                    output.close();
                    return;
                }
                System.out.println("Recv from client: " + command);
                String token = command.split(" ")[0];
                if (token.equals(Protocol.STORE_TOKEN) || token.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                    handleStore(command);
                } else if (token.equals(Protocol.REBALANCE_STORE_TOKEN)) {
                    handleRebalanceStore(command);
                } else if (token.equals(Protocol.LOAD_DATA_TOKEN)) {
                    handleDataLoad(command);
                } else if (token.equals(Protocol.REMOVE_TOKEN)) {
                    handleRemove(command);
                } else if (token.equals(Protocol.LIST_TOKEN)) {
                    handleList(command);
                } else if (token.equals(Protocol.REBALANCE_TOKEN)) {
                    handleRebalance(command);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle command STORE.
     * 
     * @param command
     */
    private void handleStore(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 3) {
            return;
        }
        String filename = tokens[1];
        int filesize = Integer.valueOf(tokens[2]);
        if (filesize < 0) {
            return;
        }
        fileContent = new FileContent(filename, filesize, dstore.getFileFolder());
        dstore.addFile(fileContent);
        output.println(Protocol.ACK_TOKEN);
        output.flush();

        fileContent.writeContent(in);

        PrintWriter controllerOutput = dstore.getControllerOutput();
        controllerOutput.println(Protocol.STORE_ACK_TOKEN + " " + fileContent.getfilename());
        controllerOutput.flush();
    }

    private void handleRebalanceStore(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 3) {
            return;
        }
        String filename = tokens[1];
        int filesize = Integer.valueOf(tokens[2]);
        if (filesize < 0) {
            return;
        }
        fileContent = new FileContent(filename, filesize, dstore.getFileFolder());
        dstore.addFile(fileContent);
        output.println(Protocol.ACK_TOKEN);
        output.flush();

        fileContent.writeContent(in);
    }

    /**
     * Handle command LOAD_DATA
     * @param command
     */
    private void handleDataLoad(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        String path = dstore.getFileFolder() + File.separator + filename;
        try {
            FileInputStream fis = new FileInputStream(new File(path));
            OutputStream os = socket.getOutputStream();
            os.write(fis.readNBytes(this.dstore.getFilesize(filename)));
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle command REMOVE.
     * 
     * @param command
     */
    private void handleRemove(String command) {
        String[] tokens = command.split(" ");
        if (tokens.length != 2) {
            return;
        }
        String filename = tokens[1];
        String path = dstore.getFileFolder() + File.separator + filename;
        File file = new File(path);
        file.delete();
        if (dstore.removeFile(filename)) {
            output.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        } else {
            output.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
        }
        output.flush();
    }

    /**
     * Handle command LIST>
     * 
     * @param command
     */
    private void handleList(String command) {
        String filenames = dstore.listFile();
        output.println(Protocol.LIST_TOKEN + " " + filenames);
        output.flush();
    }

    /**
     * Handle command REBALANCE.
     * 
     * @param command
     */
    private void handleRebalance(String command) {
        String[] parts = command.split(" ");
        int num = 1;
        int numberToSend = Integer.valueOf(parts[num++]);
        for (int i = 0; i < numberToSend; i++) {
            String filename = parts[num++];
            int portToSend = Integer.valueOf(parts[num++]);
            for (int j = 0; j < portToSend; j++) {
                int dport = Integer.valueOf(parts[num++]);
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("127.0.0.1", dport), this.dstore.getTimeout());
                    BufferedReader dinput = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter doutput = new PrintWriter(socket.getOutputStream());
                    doutput.println(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + this.dstore.getFilesize(filename));
                    doutput.flush();
                    String response = dinput.readLine();
                    if (response.startsWith(Protocol.ACK_TOKEN)) {
                        String path = dstore.getFileFolder() + File.separator + filename;
                        
                        FileInputStream fis = new FileInputStream(new File(path));
                        OutputStream os = socket.getOutputStream();
                        os.write(fis.readNBytes(this.dstore.getFilesize(filename)));
                        os.flush();
                    }
                    dinput.close();
                    doutput.close();
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        int numberToRemove = Integer.valueOf(parts[num++]);
        for (int i = 0; i < numberToRemove; i++) {
            String filename = parts[num++];
            this.dstore.removeFile(filename);
        }
        output.println(Protocol.REBALANCE_COMPLETE_TOKEN);
        output.flush();
    }
}

/**
 * Class dstore.
 */
public class Dstore {

    private int port;
    private int cport;
    private int timeout;
    private String fileFolder;
    private HashMap<String, FileContent> files;
    private BufferedReader controllerInput;
    private PrintWriter controllerOutput;
    private ReadWriteLock readWriteLock;

    /**
     * Constructor.
     * 
     * @param port
     * @param cport
     * @param timeout
     * @param fileFolder
     */
    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.files = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();

        this.initFileFolder();
        this.handleController();
        this.handleRequest();
    }

    /**
     * Get controll port.
     * 
     * @return
     */
    public int getCport() {
        return cport;
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
     * Get timeout.
     * 
     * @return
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * Get file folder.
     * 
     * @return
     */
    public String getFileFolder() {
        return fileFolder;
    }

    /**
     * Set controller input.
     * 
     * @param controllerInput
     */
    public void setControllerInput(BufferedReader controllerInput) {
        this.controllerInput = controllerInput;
    }

    /**
     * Set controller output.
     * 
     * @param controllerOutput
     */
    public void setControllerOutput(PrintWriter controllerOutput) {
        this.controllerOutput = controllerOutput;
    }

    /**
     * Get controller input.
     * 
     * @return
     */
    public BufferedReader getControllerInput() {
        return controllerInput;
    }

    /**
     * Get controller output.
     * 
     * @return
     */
    public PrintWriter getControllerOutput() {
        return controllerOutput;
    }

    /**
     * Add file.
     * 
     * @param fileContent
     */
    public void addFile(FileContent fileContent) {
        readWriteLock.writeLock().lock();
        this.files.put(fileContent.getfilename(), fileContent);
        readWriteLock.writeLock().unlock();
    }

    /**
     * Remove file.
     * 
     * @param filename
     * @return
     */
    public boolean removeFile(String filename) {
        boolean ret = false;
        readWriteLock.writeLock().lock();
        if (this.files.containsKey(filename)) {
            this.files.remove(filename);
            ret = true;
        }
        readWriteLock.writeLock().unlock();
        return ret;
    }

    /**
     * List file.
     * 
     * @return
     */
    public String listFile() {
        readWriteLock.readLock().lock();
        StringBuilder stringBuilder = new StringBuilder();
        for (String filename: files.keySet()) {
            stringBuilder.append(filename);
            stringBuilder.append(" ");
        }
        readWriteLock.readLock().unlock();
        return stringBuilder.toString().trim();
    }

    /**
     * Get file size.
     * 
     * @param filename
     * @return
     */
    public int getFilesize(String filename) {
        readWriteLock.readLock().lock();
        int size = -1;
        if (files.containsKey(filename)) {
            size = files.get(filename).getFilesize();
        }
        readWriteLock.readLock().unlock();
        return size;
    }

    /**
     * Delete dir.
     * 
     * @param file
     */
    private void deleteDir(File file) {
        File files[] = file.listFiles();
        for (File f: files) {
            if (f.isDirectory()) {
                deleteDir(f);
            } else {
                f.delete();
            }
        }
    }

    /**
     * Init file folder.
     */
    private void initFileFolder() {
        File folder = new File(fileFolder);
        if (folder.exists()) {
            deleteDir(folder);
            folder.delete();
        }
        if (!folder.mkdirs()) {
            System.err.println("Create " + fileFolder + " failed, exit.");
            System.exit(0);
        }
    }

    /**
     * Handle controller thread.
     * 
     */
    private void handleController() {
        ControllerThread controllerThread = new ControllerThread(this);
        controllerThread.start();
    }

    /**
     * Handle request thread.
     * 
     */
    private void handleRequest() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Dstore server listen on port: " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                StoreThread StoreThread = new StoreThread(socket, this);
                StoreThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            return;
        }
        int port = Integer.valueOf(args[0]);
        int cport = Integer.valueOf(args[1]);
        int timeout = Integer.valueOf(args[2]);
        String fileFolder = args[3];

        new Dstore(port, cport, timeout, fileFolder);
    }
}
