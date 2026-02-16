package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Message Format (Total: variable length):
 * - Magic Number (4 bytes): 0xDEADBEEF
 * - Version (1 byte): 1
 * - Message Type (1 byte): 0=REGISTER, 1=TASK, 2=RESULT, 3=HEARTBEAT, 4=ACK
 * - Student ID Length (2 bytes)
 * - Student ID (variable)
 * - Sender ID Length (2 bytes)
 * - Sender ID (variable)
 * - Task ID (4 bytes)
 * - Payload Length (4 bytes)
 * - Payload (variable)
 * - Timestamp (8 bytes)
 */
public class Message {
    // Message types
    public static final byte TYPE_REGISTER = 0;
    public static final byte TYPE_TASK = 1;
    public static final byte TYPE_RESULT = 2;
    public static final byte TYPE_HEARTBEAT = 3;
    public static final byte TYPE_ACK = 4;
    public static final byte TYPE_MATRIX_B = 5;
    
    // Protocol constants
    private static final int MAGIC_NUMBER = 0xDEADBEEF;
    private static final byte VERSION = 1;
    
    // Message fields
    private byte messageType;
    private String studentId;
    private String sender;
    private int taskId;
    private byte[] payload;
    private long timestamp;

    public Message(byte messageType, String studentId, String sender, int taskId, byte[] payload) {
        this.messageType = messageType;
        this.studentId = studentId != null ? studentId : System.getenv("STUDENT_ID");
        if (this.studentId == null) this.studentId = "unknown";
        this.sender = sender != null ? sender : "";
        this.taskId = taskId;
        this.payload = payload != null ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    public byte[] pack() {
        byte[] studentIdBytes = studentId.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
        
        int totalSize = 
            4 + // Magic
            1 + // Version
            1 + // Message Type
            2 + studentIdBytes.length + // Student ID
            2 + senderBytes.length + // Sender
            4 + // Task ID
            4 + payload.length + // Payload
            8; // Timestamp
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        buffer.putInt(MAGIC_NUMBER);
        buffer.put(VERSION);
        buffer.put(messageType);
        buffer.putShort((short) studentIdBytes.length);
        buffer.put(studentIdBytes);
        buffer.putShort((short) senderBytes.length);
        buffer.put(senderBytes);
        buffer.putInt(taskId);
        buffer.putInt(payload.length);
        buffer.put(payload);
        buffer.putLong(timestamp);
        
        return buffer.array();
    }

    public static Message unpack(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid magic number");
        }
        
        byte version = buffer.get();
        byte messageType = buffer.get();
        
        short studentIdLen = buffer.getShort();
        byte[] studentIdBytes = new byte[studentIdLen];
        buffer.get(studentIdBytes);
        String studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
        
        short senderLen = buffer.getShort();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        String sender = new String(senderBytes, StandardCharsets.UTF_8);
        
        int taskId = buffer.getInt();
        
        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        
        long timestamp = buffer.getLong();
        
        Message msg = new Message(messageType, studentId, sender, taskId, payload);
        msg.timestamp = timestamp;
        return msg;
    }

    public static Message readFromStream(InputStream in) throws IOException {
        // Read magic, version, type, studentIdLen (8 bytes)
        byte[] header = new byte[8];
        int bytesRead = 0;
        while (bytesRead < 8) {
            int result = in.read(header, bytesRead, 8 - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        ByteBuffer headerBuf = ByteBuffer.wrap(header);
        headerBuf.order(ByteOrder.BIG_ENDIAN);
        headerBuf.getInt();
        headerBuf.get();
        headerBuf.get();
        short studentIdLen = headerBuf.getShort();
        
        // Read student ID
        byte[] studentIdBytes = new byte[studentIdLen];
        bytesRead = 0;
        while (bytesRead < studentIdLen) {
            int result = in.read(studentIdBytes, bytesRead, studentIdLen - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        // Read sender length
        byte[] senderLenBytes = new byte[2];
        bytesRead = 0;
        while (bytesRead < 2) {
            int result = in.read(senderLenBytes, bytesRead, 2 - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        short senderLen = ByteBuffer.wrap(senderLenBytes).order(ByteOrder.BIG_ENDIAN).getShort();
        
        // Read sender
        byte[] senderBytes = new byte[senderLen];
        bytesRead = 0;
        while (bytesRead < senderLen) {
            int result = in.read(senderBytes, bytesRead, senderLen - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        // Read taskId and payloadLen
        byte[] taskAndPayload = new byte[8];
        bytesRead = 0;
        while (bytesRead < 8) {
            int result = in.read(taskAndPayload, bytesRead, 8 - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        ByteBuffer taskBuf = ByteBuffer.wrap(taskAndPayload);
        taskBuf.order(ByteOrder.BIG_ENDIAN);
        int taskId = taskBuf.getInt();
        int payloadLen = taskBuf.getInt();
        
        // Read payload
        byte[] payload = new byte[payloadLen];
        bytesRead = 0;
        while (bytesRead < payloadLen) {
            int result = in.read(payload, bytesRead, payloadLen - bytesRead);
            if (result == -1) throw new EOFException();
            bytesRead += result;
        }
        
        // Reconstruct
        ByteBuffer complete = ByteBuffer.allocate(8 + studentIdLen + 2 + senderLen + 8 + payloadLen);
        complete.order(ByteOrder.BIG_ENDIAN);
        complete.put(header);
        complete.put(studentIdBytes);
        complete.put(senderLenBytes);
        complete.put(senderBytes);
        complete.put(taskAndPayload);
        complete.put(payload);
        
        return unpack(complete.array());
    }

    public static void writeFrame(OutputStream out, Message msg) throws IOException {
        out.write(msg.pack());
        out.flush();
    }

    // Getters
    public byte getMessageType() { return messageType; }
    public String getStudentId() { return studentId; }
    public String getSender() { return sender; }
    public int getTaskId() { return taskId; }
    public byte[] getPayload() { return payload; }
    public long getTimestamp() { return timestamp; }
}