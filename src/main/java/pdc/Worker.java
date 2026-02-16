package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.*;

public class Worker {
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private String workerId;
    private String studentId;
    private boolean running = true;
    
    private int[][] matrixB;
    private Message currentTask;
    
    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);
    private final ScheduledExecutorService heartbeatPool = Executors.newSingleThreadScheduledExecutor();
    private final BlockingQueue<Message> requestQueue = new LinkedBlockingQueue<>();

    public Worker() {
        studentId = System.getenv("STUDENT_ID");
        if (studentId == null) studentId = "unknown";
        
        workerId = System.getenv("WORKER_ID");
        if (workerId == null) workerId = "worker-" + System.currentTimeMillis();
    }

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            in = socket.getInputStream();
            out = socket.getOutputStream();
            
            // Perform handshake
            Message handshake = new Message(Message.TYPE_REGISTER, studentId, workerId, -1, new byte[0]);
            Message.writeFrame(out, handshake);
            
            // Wait for acknowledgment
            Message response = Message.readFromStream(in);
            
            if (response.getMessageType() != Message.TYPE_ACK) {
                throw new IOException("Invalid handshake response");
            }
            
            // Start heartbeat
            heartbeatPool.scheduleAtFixedRate(this::sendHeartbeat, 1, 2, TimeUnit.SECONDS);
            
            // Start request processor
            startRequestProcessor();
            
            // Process messages
            processMessages();
            
        } catch (IOException e) {
            e.printStackTrace();
            shutdown();
        }
    }

    private void startRequestProcessor() {
        Thread processor = new Thread(() -> {
            while (running) {
                try {
                    Message request = requestQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        processRequest(request);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        processor.setDaemon(true);
        processor.start();
    }

    private void processMessages() throws IOException {
        while (running) {
            try {
                Message msg = Message.readFromStream(in);
                if (msg == null) break;
                requestQueue.offer(msg);
            } catch (EOFException e) {
                break;
            }
        }
    }

    private void processRequest(Message msg) {
        try {
            switch (msg.getMessageType()) {
                case 5: // Matrix B
                    receiveMatrixB(msg);
                    break;
                    
                case Message.TYPE_TASK:
                    currentTask = msg;
                    execute();
                    break;
                    
                case Message.TYPE_HEARTBEAT:
                    Message hbAck = new Message(Message.TYPE_ACK, studentId, workerId, -1, new byte[0]);
                    synchronized (out) {
                        Message.writeFrame(out, hbAck);
                    }
                    break;
                    
                case Message.TYPE_ACK:
                    // Handle acknowledgment
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void receiveMatrixB(Message msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getPayload());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        int size = (int) Math.sqrt(msg.getPayload().length / 4);
        matrixB = new int[size][size];
        
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                matrixB[i][j] = buffer.getInt();
            }
        }
    }

    public void execute() {
        if (currentTask != null && matrixB != null) {
            taskPool.submit(this::executeTask);
        }
    }

    private void executeTask() {
        try {
            Message task = currentTask;
            
            ByteBuffer buffer = ByteBuffer.wrap(task.getPayload());
            buffer.order(ByteOrder.BIG_ENDIAN);
            
            int startRow = buffer.getInt();
            int endRow = buffer.getInt();
            int startCol = buffer.getInt();
            int endCol = buffer.getInt();
            
            int rows = endRow - startRow;
            int cols = endCol - startCol;
            
            // Extract matrix A block
            int[][] aBlock = new int[rows][cols];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    aBlock[i][j] = buffer.getInt();
                }
            }
            
            // Compute result
            int[][] result = new int[rows][cols];
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    int sum = 0;
                    for (int k = 0; k < matrixB.length; k++) {
                        sum += aBlock[i][k] * matrixB[k][startCol + j];
                    }
                    result[i][j] = sum;
                }
            }
            
            // Package result
            ByteBuffer resultBuf = ByteBuffer.allocate(16 + rows * cols * 4);
            resultBuf.order(ByteOrder.BIG_ENDIAN);
            resultBuf.putInt(startRow);
            resultBuf.putInt(endRow);
            resultBuf.putInt(startCol);
            resultBuf.putInt(endCol);
            
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    resultBuf.putInt(result[i][j]);
                }
            }
            
            // Send result
            Message resultMsg = new Message(Message.TYPE_RESULT, studentId, workerId, task.getTaskId(), resultBuf.array());
            synchronized (out) {
                Message.writeFrame(out, resultMsg);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendHeartbeat() {
        try {
            if (running && socket != null && !socket.isClosed()) {
                Message heartbeat = new Message(Message.TYPE_HEARTBEAT, studentId, workerId, -1, new byte[0]);
                synchronized (out) {
                    Message.writeFrame(out, heartbeat);
                }
            }
        } catch (IOException e) {
            running = false;
        }
    }

    public void shutdown() {
        running = false;
        heartbeatPool.shutdownNow();
        taskPool.shutdownNow();
        try {
            if (socket != null) socket.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java pdc.Worker <master_host> <master_port>");
            System.exit(1);
        }
        
        Worker worker = new Worker();
        Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
        worker.joinCluster(args[0], Integer.parseInt(args[1]));
    }
}