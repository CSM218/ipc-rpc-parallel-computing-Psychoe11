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
    private volatile boolean running = true;

    private volatile int[][] matrixB;

    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);
    private final ScheduledExecutorService heartbeatPool = Executors.newSingleThreadScheduledExecutor();
    private final BlockingQueue<Message> requestQueue = new LinkedBlockingQueue<>();

    public Worker() {

        studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "unknown";
        }

        workerId = System.getenv("WORKER_ID");
        if (workerId == null) {
            workerId = "worker-" + System.currentTimeMillis();
        }
    }

    public void joinCluster(String masterHost, int port) {

        try {

            socket = new Socket(masterHost, port);
            socket.setTcpNoDelay(true);

            in = socket.getInputStream();
            out = socket.getOutputStream();

            Message handshake = new Message(
                    Message.TYPE_REGISTER,
                    studentId,
                    workerId,
                    -1,
                    new byte[0]
            );

            synchronized (out) {
                Message.writeFrame(out, handshake);
                out.flush();
            }

            Message response = Message.readFromStream(in);

            if (response == null || response.getMessageType() != Message.TYPE_ACK) {
                throw new IOException("Invalid handshake response");
            }

            heartbeatPool.scheduleAtFixedRate(this::sendHeartbeat, 1, 2, TimeUnit.SECONDS);

            startRequestProcessor();
            processMessages();

        } catch (IOException e) {
            // connection lost
        } finally {
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
                    executeTask(msg);
                    break;

                case Message.TYPE_HEARTBEAT:
                    sendAck();
                    break;

                case Message.TYPE_ACK:
                    break;
            }

        } catch (Exception e) {
            // ignore task-level failure
        }
    }

    private void receiveMatrixB(Message msg) {

        ByteBuffer buffer = ByteBuffer.wrap(msg.getPayload());
        buffer.order(ByteOrder.BIG_ENDIAN);

        int size = (int) Math.sqrt(msg.getPayload().length / 4);
        int[][] localB = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                localB[i][j] = buffer.getInt();
            }
        }

        matrixB = localB;
    }

    private void executeTask(Message task) {

        if (matrixB == null) return;

        taskPool.submit(() -> {

            try {

                ByteBuffer buffer = ByteBuffer.wrap(task.getPayload());
                buffer.order(ByteOrder.BIG_ENDIAN);

                int startRow = buffer.getInt();
                int endRow = buffer.getInt();
                int startCol = buffer.getInt();
                int endCol = buffer.getInt();

                int rows = endRow - startRow;
                int cols = endCol - startCol;
                int n = matrixB.length;

                int[][] aBlock = new int[rows][n];

                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < n; j++) {
                        aBlock[i][j] = buffer.getInt();
                    }
                }

                int[][] result = new int[rows][cols];

                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {

                        int sum = 0;
                        for (int k = 0; k < n; k++) {
                            sum += aBlock[i][k] * matrixB[k][startCol + j];
                        }

                        result[i][j] = sum;
                    }
                }

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

                Message resultMsg = new Message(
                        Message.TYPE_RESULT,
                        studentId,
                        workerId,
                        task.getTaskId(),
                        resultBuf.array()
                );

                synchronized (out) {
                    Message.writeFrame(out, resultMsg);
                    out.flush();
                }

            } catch (Exception ignored) {
            }
        });
    }

    private void sendHeartbeat() {

        try {

            if (!running || socket == null || socket.isClosed()) return;

            Message heartbeat = new Message(
                    Message.TYPE_HEARTBEAT,
                    studentId,
                    workerId,
                    -1,
                    new byte[0]
            );

            synchronized (out) {
                Message.writeFrame(out, heartbeat);
                out.flush();
            }

        } catch (IOException e) {
            running = false;
        }
    }

    private void sendAck() throws IOException {

        Message ack = new Message(
                Message.TYPE_ACK,
                studentId,
                workerId,
                -1,
                new byte[0]
        );

        synchronized (out) {
            Message.writeFrame(out, ack);
            out.flush();
        }
    }

    public void shutdown() {

        running = false;

        heartbeatPool.shutdownNow();
        taskPool.shutdownNow();

        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Non-blocking execute entry used by tests to start worker internals.
     */
    public void execute() {
        // start internal request processor so this method is non-blocking
        startRequestProcessor();
        // spawn a short-lived thread to keep the worker alive without blocking
        Thread t = new Thread(() -> {
            try {
                while (running) {
                    Thread.sleep(100);
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
        t.setDaemon(true);
        t.start();
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
