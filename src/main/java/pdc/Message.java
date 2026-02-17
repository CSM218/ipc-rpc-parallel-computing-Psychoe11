package pdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;

public class Message {

    public static final byte TYPE_REGISTER = 0;
    public static final byte TYPE_TASK = 1;
    public static final byte TYPE_RESULT = 2;
    public static final byte TYPE_HEARTBEAT = 3;
    public static final byte TYPE_ACK = 4;
    public static final byte TYPE_MATRIX_B = 5;

    private static final int MAGIC_NUMBER = 0xDEADBEEF;
    private static final byte VERSION = 1;

    private byte messageType;
    private String studentId;
    private String sender;
    private int taskId;
    private byte[] payload;
    private long timestamp;

    private static final Map<Byte, String> TYPE_TO_NAME = new HashMap<>();
    private static final Map<String, Byte> NAME_TO_TYPE = new HashMap<>();

    static {
        TYPE_TO_NAME.put(TYPE_REGISTER, "REGISTER_WORKER");
        TYPE_TO_NAME.put(TYPE_TASK, "RPC_REQUEST");
        TYPE_TO_NAME.put(TYPE_RESULT, "RPC_RESPONSE");
        TYPE_TO_NAME.put(TYPE_HEARTBEAT, "HEARTBEAT");
        TYPE_TO_NAME.put(TYPE_ACK, "WORKER_ACK");
        TYPE_TO_NAME.put(TYPE_MATRIX_B, "MATRIX_B");

        for (Map.Entry<Byte, String> e : TYPE_TO_NAME.entrySet()) {
            NAME_TO_TYPE.put(e.getValue(), e.getKey());
        }
    }

    public Message(byte messageType, String studentId, String sender, int taskId, byte[] payload) {

        this.messageType = messageType;
        this.studentId = studentId != null ? studentId : System.getenv("STUDENT_ID");
        if (this.studentId == null) this.studentId = "unknown";

        this.sender = sender != null ? sender : "";
        this.taskId = taskId;
        this.payload = payload != null ? payload : new byte[0];
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Serialize this message into the JSON protocol described in ASSIGNMENT.md.
     * Payload is Base64-encoded into the `payload` field.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        String typeName = TYPE_TO_NAME.getOrDefault(messageType, Byte.toString(messageType));
        String payloadStr = Base64.getEncoder().encodeToString(payload != null ? payload : new byte[0]);

        sb.append('{');
        sb.append("\"magic\":\"CSM218\",");
        sb.append("\"version\":1,");
        sb.append("\"messageType\":\"").append(escapeJson(typeName)).append('\"').append(',');
        sb.append("\"studentId\":\"").append(escapeJson(studentId)).append('\"').append(',');
        sb.append("\"timestamp\":").append(timestamp).append(',');
        sb.append("\"payload\":\"").append(escapeJson(payloadStr)).append('\"');
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    /**
     * Parse a JSON protocol message into a Message instance.
     * This is a small parser intended for the assignment tests and not a full JSON parser.
     */
    public static Message parse(String json) throws IOException {
        if (json == null) throw new IOException("null json");
        String compact = json.trim();

        String magic = extractStringField(compact, "magic");
        if (magic == null) throw new IOException("missing magic");

        String versionStr = extractNumberField(compact, "version");
        int version = versionStr != null ? Integer.parseInt(versionStr) : -1;

        String messageTypeName = extractStringField(compact, "messageType");
        String student = extractStringField(compact, "studentId");
        String timestampStr = extractNumberField(compact, "timestamp");
        long ts = timestampStr != null ? Long.parseLong(timestampStr) : System.currentTimeMillis();

        String payloadB64 = extractStringField(compact, "payload");
        byte[] payloadBytes = payloadB64 != null ? Base64.getDecoder().decode(payloadB64) : new byte[0];

        byte type = NAME_TO_TYPE.getOrDefault(messageTypeName, (byte) 0);

        Message m = new Message(type, student, "", -1, payloadBytes);
        m.timestamp = ts;
        return m;
    }

    private static String extractStringField(String s, String field) {
        String key = "\"" + field + "\":";
        int idx = s.indexOf(key);
        if (idx == -1) return null;
        int start = s.indexOf('"', idx + key.length());
        if (start == -1) return null;
        start++;
        int end = s.indexOf('"', start);
        if (end == -1) return null;
        String raw = s.substring(start, end);
        return raw.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private static String extractNumberField(String s, String field) {
        String key = "\"" + field + "\":";
        int idx = s.indexOf(key);
        if (idx == -1) return null;
        int start = idx + key.length();
        // read until comma or closing brace
        int end = start;
        while (end < s.length() && (Character.isDigit(s.charAt(end)) || s.charAt(end) == '-')) end++;
        if (end == start) return null;
        return s.substring(start, end);
    }

    /**
     * Validate basic protocol fields for this message instance.
     */
    public void validate() throws Exception {
        if (studentId == null || studentId.isEmpty()) throw new Exception("studentId missing");
        if (timestamp <= 0) throw new Exception("invalid timestamp");
    }

    public byte[] pack() {

        byte[] studentIdBytes = studentId.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);

        int totalSize =
                4 + // magic
                1 + // version
                1 + // type
                2 + studentIdBytes.length +
                2 + senderBytes.length +
                4 + // taskId
                4 + payload.length +
                8; // timestamp

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

    public static Message readFromStream(InputStream in) throws IOException {

        byte[] fixedHeader = readExactly(in, 8);

        ByteBuffer headerBuf = ByteBuffer.wrap(fixedHeader);
        headerBuf.order(ByteOrder.BIG_ENDIAN);

        int magic = headerBuf.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: 0x" + Integer.toHexString(magic));
        }

        byte version = headerBuf.get();
        if (version != VERSION) {
            throw new IOException("Unsupported protocol version: " + version);
        }

        byte messageType = headerBuf.get();
        short studentIdLen = headerBuf.getShort();

        if (studentIdLen < 0 || studentIdLen > 4096) {
            throw new IOException("Invalid studentId length: " + studentIdLen);
        }

        byte[] studentIdBytes = readExactly(in, studentIdLen);

        short senderLen = ByteBuffer.wrap(readExactly(in, 2))
                .order(ByteOrder.BIG_ENDIAN)
                .getShort();

        if (senderLen < 0 || senderLen > 4096) {
            throw new IOException("Invalid sender length: " + senderLen);
        }

        byte[] senderBytes = readExactly(in, senderLen);

        ByteBuffer taskBuf = ByteBuffer.wrap(readExactly(in, 8));
        taskBuf.order(ByteOrder.BIG_ENDIAN);

        int taskId = taskBuf.getInt();
        int payloadLen = taskBuf.getInt();

        if (payloadLen < 0 || payloadLen > 50_000_000) {
            throw new IOException("Invalid payload length: " + payloadLen);
        }

        byte[] payload = readExactly(in, payloadLen);

        long timestamp = ByteBuffer.wrap(readExactly(in, 8))
                .order(ByteOrder.BIG_ENDIAN)
                .getLong();

        String studentId = new String(studentIdBytes, StandardCharsets.UTF_8);
        String sender = new String(senderBytes, StandardCharsets.UTF_8);

        Message msg = new Message(messageType, studentId, sender, taskId, payload);
        msg.timestamp = timestamp;

        return msg;
    }

    private static byte[] readExactly(InputStream in, int n) throws IOException {

        if (n == 0) return new byte[0];

        byte[] buffer = new byte[n];
        int totalRead = 0;

        while (totalRead < n) {

            int read = in.read(buffer, totalRead, n - totalRead);

            if (read == -1) {
                throw new EOFException(
                        "Stream ended prematurely. Expected " + n + " bytes, got " + totalRead
                );
            }

            totalRead += read;
        }

        return buffer;
    }

    public static void writeFrame(OutputStream out, Message msg) throws IOException {
        byte[] data = msg.pack();
        out.write(data);
        // DO NOT flush here — caller controls flush timing
    }

    public byte getMessageType() { return messageType; }
    public String getStudentId() { return studentId; }
    public String getSender() { return sender; }
    public int getTaskId() { return taskId; }
    public byte[] getPayload() { return payload; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + messageType +
                ", student=" + studentId +
                ", sender=" + sender +
                ", taskId=" + taskId +
                ", payloadSize=" + payload.length +
                ", timestamp=" + timestamp +
                '}';
    }
}
