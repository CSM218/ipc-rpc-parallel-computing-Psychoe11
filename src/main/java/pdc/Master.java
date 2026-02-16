package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final ScheduledExecutorService monitorPool = Executors.newScheduledThreadPool(3);
    
    private int[][] matrixA;
    private int[][] matrixB;
    private int[][] resultMatrix;
    private int totalTasks;
    private int blockSize = 10;
    private String studentId;
    
    private static final long HEARTBEAT_TIMEOUT = 5000;
    private static final long TASK_TIMEOUT = 10000;
    private static final int MAX_RETRIES = 3;

    public Master() {
        studentId = System.getenv("STUDENT_ID");
        if (studentId == null) studentId = "unknown";
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (!"BLOCK_MULTIPLY".equals(operation)) {
            throw new IllegalArgumentException("Unsupported operation");
        }
        
        try {
            // Split data into matrices A and B
            int n = data.length / 2;
            matrixA = Arrays.copyOfRange(data, 0, n);
            matrixB = Arrays.copyOfRange(data, n, 2 * n);
            resultMatrix = new int[n][n];
            
            // Start monitoring
            monitorPool.scheduleAtFixedRate(this::checkHeartbeats, 1, 1, TimeUnit.SECONDS);
            monitorPool.scheduleAtFixedRate(this::checkTaskTimeouts, 1, 1, TimeUnit.SECONDS);
            monitorPool.scheduleAtFixedRate(this::recoveryMechanism, 1, 1, TimeUnit.SECONDS);
            
            // Wait for workers
            waitForWorkers(workerCount, 30000);
            
            if (workers.isEmpty()) {
                throw new RuntimeException("No workers available");
            }
            
            // Distribute matrix B
            broadcastMatrixB();
            
            // Create and distribute tasks
            createTasks();
            
            // Wait for completion
            long startTime = System.currentTimeMillis();
            long timeout = 60000;
            
            while (results.size() < totalTasks && System.currentTimeMillis() - startTime < timeout) {
                Thread.sleep(100);
            }
            
            // Assemble result
            assembleResult();
            
            return resultMatrix;
            
        } catch (Exception e) {
            throw new RuntimeException("Coordinate failed: " + e.getMessage(), e);
        }
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
        
        Message bMsg = new Message((byte)5, studentId, "master", -1, buffer.array());
        
        for (WorkerInfo worker : workers.values()) {
            try {
                Message.writeFrame(worker.out, bMsg);
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
                
                ByteBuffer buffer = ByteBuffer.allocate(16 + rows * cols * 4);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.putInt(i);
                buffer.putInt(rowEnd);
                buffer.putInt(j);
                buffer.putInt(colEnd);
                
                for (int r = i; r < rowEnd; r++) {
                    for (int c = j; c < colEnd; c++) {
                        buffer.putInt(matrixA[r][c]);
                    }
                }
                
                Task task = new Task(taskId, buffer.array(), i, j, rows, cols);
                pendingTasks.offer(task);
                taskId++;
            }
        }
        
        totalTasks = taskId;
        
        // Start assigning tasks
        for (WorkerInfo worker : workers.values()) {
            assignTasks(worker);
        }
    }

    private void assignTasks(WorkerInfo worker) {
        while (worker.assignedTasks.size() < 2) {
            Task task = pendingTasks.poll();
            if (task == null) break;
            
            try {
                Message taskMsg = new Message(Message.TYPE_TASK, studentId, "master", task.taskId, task.data);
                Message.writeFrame(worker.out, taskMsg);
                
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

    private void handleResult(Message msg, WorkerInfo worker) {
        int taskId = msg.getTaskId();
        
        worker.assignedTasks.remove((Integer) taskId);
        assignedTasks.remove(taskId);
        assignmentTime.remove(taskId);
        retryCount.remove(taskId);
        
        ByteBuffer buffer = ByteBuffer.wrap(msg.getPayload());
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        int startRow = buffer.getInt();
        int endRow = buffer.getInt();
        int startCol = buffer.getInt();
        int endCol = buffer.getInt();
        
        byte[] data = new byte[msg.getPayload().length - 16];
        System.arraycopy(msg.getPayload(), 16, data, 0, data.length);
        
        TaskResult result = new TaskResult(taskId, data, startRow, startCol, 
                                           endRow - startRow, endCol - startCol);
        results.put(taskId, result);
        
        // Assign next task
        assignTasks(worker);
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
            handleFailedTask(taskId, "timeout");
        }
    }

    private void recoveryMechanism() {
        // Check for tasks assigned to dead workers
        for (Map.Entry<Integer, Task> entry : assignedTasks.entrySet()) {
            int taskId = entry.getKey();
            boolean workerAlive = false;
            
            for (WorkerInfo worker : workers.values()) {
                if (worker.assignedTasks.contains(taskId)) {
                    workerAlive = true;
                    break;
                }
            }
            
            if (!workerAlive) {
                handleFailedTask(taskId, "worker_dead");
            }
        }
    }

    private void handleFailedTask(int taskId, String reason) {
        Task task = assignedTasks.remove(taskId);
        assignmentTime.remove(taskId);
        
        if (task != null) {
            int retries = retryCount.getOrDefault(taskId, 0);
            if (retries < MAX_RETRIES) {
                retryCount.put(taskId, retries + 1);
                pendingTasks.offer(task);
            }
        }
    }

    private void removeWorker(String workerId) {
        WorkerInfo worker = workers.remove(workerId);
        if (worker != null) {
            // Reassign tasks
            List<Integer> tasksToReassign = new ArrayList<>(worker.assignedTasks);
            for (Integer taskId : tasksToReassign) {
                handleFailedTask(taskId, "worker_disconnected");
            }
            
            try {
                worker.socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
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

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                workerPool.submit(new WorkerHandler(socket));
            } catch (IOException e) {
                if (running) e.printStackTrace();
            }
        }
    }

    public void reconcileState() {
        checkHeartbeats();
        checkTaskTimeouts();
        recoveryMechanism();
    }

    private class WorkerHandler implements Runnable {
        private final Socket socket;
        private WorkerInfo workerInfo;
        
        WorkerHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                
                Message handshake = Message.readFromStream(in);
                
                if (handshake.getMessageType() != Message.TYPE_REGISTER) {
                    socket.close();
                    return;
                }
                
                String workerId = handshake.getSender();
                workerInfo = new WorkerInfo(workerId, socket, in, out);
                workers.put(workerId, workerInfo);
                
                // Send acknowledgment
                Message ack = new Message(Message.TYPE_ACK, studentId, "master", -1, new byte[0]);
                Message.writeFrame(out, ack);
                
                // Process messages
                while (running && !socket.isClosed()) {
                    try {
                        Message msg = Message.readFromStream(in);
                        if (msg == null) break;
                        
                        workerInfo.lastHeartbeat = System.currentTimeMillis();
                        
                        if (msg.getMessageType() == Message.TYPE_RESULT) {
                            handleResult(msg, workerInfo);
                        } else if (msg.getMessageType() == Message.TYPE_HEARTBEAT) {
                            Message hbAck = new Message(Message.TYPE_ACK, studentId, "master", -1, new byte[0]);
                            Message.writeFrame(out, hbAck);
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }
            } catch (IOException e) {
                // Worker disconnected
            } finally {
                if (workerInfo != null) {
                    removeWorker(workerInfo.id);
                }
            }
        }
    }

    private static class WorkerInfo {
        final String id;
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        volatile long lastHeartbeat;
        final List<Integer> assignedTasks = new ArrayList<>();
        
        WorkerInfo(String id, Socket socket, InputStream in, OutputStream out) {
            this.id = id;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }

    private static class Task {
        final int taskId;
        final byte[] data;
        final int startRow, startCol, rows, cols;
        
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
        final int taskId;
        final byte[] data;
        final int startRow, startCol, rows, cols;
        
        TaskResult(int taskId, byte[] data, int startRow, int startCol, int rows, int cols) {
            this.taskId = taskId;
            this.data = data;
            this.startRow = startRow;
            this.startCol = startCol;
            this.rows = rows;
            this.cols = cols;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: java pdc.Master <port>");
            System.exit(1);
        }
        
        Master master = new Master();
        master.listen(Integer.parseInt(args[0]));
    }
}