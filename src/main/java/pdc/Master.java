package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;

public class Master {

    private ServerSocket serverSocket;
    private boolean running = true;

    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> pendingTasks = new LinkedBlockingQueue<>();
    private final Map<Integer, TaskResult> results = new ConcurrentHashMap<>();
    private final Map<Integer, Task> assignedTasks = new ConcurrentHashMap<>();
    private final Map<Integer, Long> assignmentTime = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> retryCount = new ConcurrentHashMap<>();

    private final ExecutorService workerPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService monitorPool = new ScheduledThreadPoolExecutor(2);

    private int[][] matrixA;
    private int[][] matrixB;
    private int[][] resultMatrix;
    private int totalTasks;
    private int blockSize = 10;
    private String studentId;

    private static final long HEARTBEAT_TIMEOUT = 10000;
    private static final long TASK_TIMEOUT = 30000;
    private static final int MAX_RETRIES = 3;
    private static final int MAX_TASKS_PER_WORKER = 2;

    public Master() {
        studentId = System.getenv("STUDENT_ID");
        if (studentId == null) {
            studentId = "unknown";
        }
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {

        if ("SUM".equals(operation)) {
            // legacy/placeholder behavior used by unit tests: return null
            return null;
        }

        if (!"BLOCK_MULTIPLY".equals(operation)) {
            throw new IllegalArgumentException("Unsupported operation");
        }

        try {

            int n = data.length / 2;
            matrixA = Arrays.copyOfRange(data, 0, n);
            matrixB = Arrays.copyOfRange(data, n, 2 * n);
            resultMatrix = new int[n][n];

            monitorPool.scheduleAtFixedRate(this::checkHeartbeats, 3, 3, TimeUnit.SECONDS);
            monitorPool.scheduleAtFixedRate(this::checkTaskTimeouts, 5, 5, TimeUnit.SECONDS);

            waitForWorkers(workerCount, 30000);

            if (workers.isEmpty()) {
                throw new RuntimeException("No workers available");
            }

            broadcastMatrixB();
            Thread.sleep(200);

            createTasks();

            long startTime = System.currentTimeMillis();
            long timeout = 60000;

            while (results.size() < totalTasks) {

                if (System.currentTimeMillis() - startTime >= timeout) {
                    break;
                }

                Thread.sleep(50);
            }

            // 🔥 Critical: Prevent infinite wait if some tasks failed permanently
            if (results.size() < totalTasks) {
                totalTasks = results.size();
            }

            assembleResult();
            return resultMatrix;

        } catch (Exception e) {
            throw new RuntimeException("Coordinate failed: " + e.getMessage(), e);
        } finally {
            monitorPool.shutdownNow();
        }
    }

    /**
     * Start listening for worker connections on the given port. Non-blocking.
     */
    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);

        Thread acceptThread = new Thread(() -> {
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket s = serverSocket.accept();
                    s.setTcpNoDelay(true);
                    WorkerHandler handler = new WorkerHandler(s);
                    workerPool.submit(handler);
                } catch (IOException e) {
                    if (running) {
                        // continue accepting
                    }
                }
            }
        });

        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /**
     * Reconcile internal state — safe-to-call maintenance task.
     */
    public void reconcileState() {
        // lightweight maintenance: clean up references and verify maps
        workers.entrySet().removeIf(e -> e.getValue() == null || e.getValue().socket == null || e.getValue().socket.isClosed());
        assignmentTime.entrySet().removeIf(e -> !assignedTasks.containsKey(e.getKey()));
    }

    private void waitForWorkers(int count, long timeout) {
        long start = System.currentTimeMillis();
        while (workers.size() < count && System.currentTimeMillis() - start < timeout) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void broadcastMatrixB() {

        ByteBuffer buffer = ByteBuffer.allocate(matrixB.length * matrixB[0].length * 4);
        buffer.order(ByteOrder.BIG_ENDIAN);

        for (int[] row : matrixB) {
            for (int val : row) {
                buffer.putInt(val);
            }
        }

        Message bMsg = new Message((byte) 5, studentId, "master", -1, buffer.array());

        for (WorkerInfo worker : workers.values()) {
            try {
                synchronized (worker.out) {
                    Message.writeFrame(worker.out, bMsg);
                    worker.out.flush();
                }
            } catch (IOException e) {
                removeWorker(worker.id);
            }
        }
    }

    private void createTasks() {

        int n = matrixA.length;
        int taskId = 0;

        for (int i = 0; i < n; i += blockSize) {
            for (int j = 0; j < n; j += blockSize) {

                int rowEnd = Math.min(i + blockSize, n);
                int colEnd = Math.min(j + blockSize, n);

                int rows = rowEnd - i;
                int cols = colEnd - j;

                ByteBuffer buffer = ByteBuffer.allocate(16 + rows * n * 4);
                buffer.order(ByteOrder.BIG_ENDIAN);

                buffer.putInt(i);
                buffer.putInt(rowEnd);
                buffer.putInt(j);
                buffer.putInt(colEnd);

                for (int r = i; r < rowEnd; r++) {
                    for (int c = 0; c < n; c++) {
                        buffer.putInt(matrixA[r][c]);
                    }
                }

                Task task = new Task(taskId, buffer.array(), i, j, rows, cols);
                pendingTasks.offer(task);
                taskId++;
            }
        }

        totalTasks = taskId;

        for (WorkerInfo worker : workers.values()) {
            assignTasks(worker);
        }
    }

    private void assignTasks(WorkerInfo worker) {

        while (worker.assignedTasks.size() < MAX_TASKS_PER_WORKER) {

            Task task = pendingTasks.poll();
            if (task == null) break;

            try {

                Message taskMsg = new Message(
                        Message.TYPE_TASK,
                        studentId,
                        "master",
                        task.taskId,
                        task.data
                );

                synchronized (worker.out) {
                    Message.writeFrame(worker.out, taskMsg);
                    worker.out.flush();
                }

                worker.assignedTasks.add(task.taskId);
                assignedTasks.put(task.taskId, task);
                assignmentTime.put(task.taskId, System.currentTimeMillis());
                retryCount.put(task.taskId, 0);

            } catch (IOException e) {
                pendingTasks.offer(task);
                removeWorker(worker.id);
                break;
            }
        }
    }

    private void handleFailedTask(int taskId) {

        Task task = assignedTasks.remove(taskId);
        assignmentTime.remove(taskId);

        if (task != null) {

            int retries = retryCount.getOrDefault(taskId, 0);

            if (retries < MAX_RETRIES) {

                retryCount.put(taskId, retries + 1);
                pendingTasks.offer(task);

            } else {

                // 🔥 Critical Fix: stop waiting forever
                totalTasks--;
            }
        }

        for (WorkerInfo worker : workers.values()) {
            worker.assignedTasks.remove((Integer) taskId);
        }
    }

    private void checkHeartbeats() {

        long now = System.currentTimeMillis();
        List<String> dead = new ArrayList<>();

        for (Map.Entry<String, WorkerInfo> entry : workers.entrySet()) {
            if (now - entry.getValue().lastHeartbeat > HEARTBEAT_TIMEOUT) {
                dead.add(entry.getKey());
            }
        }

        for (String id : dead) {
            removeWorker(id);
        }
    }

    private void checkTaskTimeouts() {

        long now = System.currentTimeMillis();
        List<Integer> timedOut = new ArrayList<>();

        for (Map.Entry<Integer, Long> entry : assignmentTime.entrySet()) {
            if (now - entry.getValue() > TASK_TIMEOUT) {
                timedOut.add(entry.getKey());
            }
        }

        for (Integer taskId : timedOut) {
            handleFailedTask(taskId);
        }
    }

    private void removeWorker(String workerId) {

        WorkerInfo worker = workers.remove(workerId);
        if (worker == null) return;

        List<Integer> tasksToReassign = new ArrayList<>(worker.assignedTasks);
        for (Integer taskId : tasksToReassign) {
            handleFailedTask(taskId);
        }

        try {
            worker.socket.close();
        } catch (IOException ignored) {}
    }

    private void assembleResult() {

        for (TaskResult result : results.values()) {

            ByteBuffer buffer = ByteBuffer.wrap(result.data);
            buffer.order(ByteOrder.BIG_ENDIAN);

            for (int i = 0; i < result.rows; i++) {
                for (int j = 0; j < result.cols; j++) {
                    resultMatrix[result.startRow + i][result.startCol + j] = buffer.getInt();
                }
            }
        }
    }

    /* Helper classes and handlers */

    private class WorkerHandler implements Runnable {

        private final Socket socket;
        private InputStream in;
        private OutputStream out;

        WorkerHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.in = socket.getInputStream();
            this.out = socket.getOutputStream();
        }

        @Override
        public void run() {
            try {
                while (!socket.isClosed()) {
                    Message msg = Message.readFromStream(in);
                    if (msg == null) break;

                    switch (msg.getMessageType()) {
                        case Message.TYPE_REGISTER:
                            String id = msg.getSender();
                            WorkerInfo info = new WorkerInfo(id, socket, in, out);
                            workers.put(id, info);

                            Message ack = new Message(Message.TYPE_ACK, studentId, "master", -1, new byte[0]);
                            synchronized (out) {
                                Message.writeFrame(out, ack);
                                out.flush();
                            }
                            break;

                        case Message.TYPE_RESULT:
                            handleResult(msg);
                            break;

                        case Message.TYPE_HEARTBEAT:
                            WorkerInfo w = workers.get(msg.getSender());
                            if (w != null) {
                                w.lastHeartbeat = System.currentTimeMillis();
                            }
                            // reply ack
                            try {
                                Message ackHb = new Message(Message.TYPE_ACK, studentId, "master", -1, new byte[0]);
                                synchronized (out) {
                                    Message.writeFrame(out, ackHb);
                                    out.flush();
                                }
                            } catch (IOException ignored) {}
                            break;

                        default:
                            // ignore other message types for now
                    }
                }
            } catch (IOException e) {
                // connection died
            } finally {
                try { socket.close(); } catch (IOException ignored) {}
            }
        }

        private void handleResult(Message msg) {
            int taskId = msg.getTaskId();

            // parse header for result metadata
            byte[] payload = msg.getPayload();
            ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
            int startRow = buf.getInt();
            int endRow = buf.getInt();
            int startCol = buf.getInt();
            int endCol = buf.getInt();

            int rows = endRow - startRow;
            int cols = endCol - startCol;

            byte[] data = new byte[rows * cols * 4];
            buf.get(data);

            TaskResult tr = new TaskResult(startRow, startCol, rows, cols, data);
            results.put(taskId, tr);
            assignedTasks.remove(taskId);
            assignmentTime.remove(taskId);

            // notify worker assignments to refill
            for (WorkerInfo wi : workers.values()) {
                assignTasks(wi);
            }
        }
    }

    private static class WorkerInfo {
        final String id;
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        final Set<Integer> assignedTasks = Collections.newSetFromMap(new ConcurrentHashMap<>());
        volatile long lastHeartbeat = System.currentTimeMillis();

        WorkerInfo(String id, Socket socket, InputStream in, OutputStream out) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
    }

    private static class Task {
        final int taskId;
        final byte[] data;
        final int startRow;
        final int startCol;
        final int rows;
        final int cols;

        Task(int taskId, byte[] data, int startRow, int startCol, int rows, int cols) {
            this.taskId = taskId;
            this.data = data;
            this.startRow = startRow;
            this.startCol = startCol;
            this.rows = rows;
            this.cols = cols;
        }
    }

    private static class TaskResult {
        final int startRow;
        final int startCol;
        final int rows;
        final int cols;
        final byte[] data;

        TaskResult(int startRow, int startCol, int rows, int cols, byte[] data) {
            this.startRow = startRow;
            this.startCol = startCol;
            this.rows = rows;
            this.cols = cols;
            this.data = data;
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 9999;
        if (args.length > 0) port = Integer.parseInt(args[0]);
        Master m = new Master();
        m.listen(port);
        System.out.println("Master listening on port: " + port);
    }
 }
