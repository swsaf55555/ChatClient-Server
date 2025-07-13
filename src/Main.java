import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import com.google.gson.*;

public class Main {
    private static String USER;
    private static String PASSWORD;


    public static void main(String[] args) throws IOException {
        List<String> logs = new CopyOnWriteArrayList<>();
        List<User> users = new CopyOnWriteArrayList<>();

        System.out.println("要监听哪个端口？");
        Scanner scanner = new Scanner(System.in);
        int port = scanner.nextInt();

        System.out.println("输入数据库用户名：");
        USER = scanner.next();
        System.out.println("请输入数据库密码：");
        PASSWORD = scanner.next();
        DB.connect(USER, PASSWORD);
        Logger.monitor(logs);
        logs.add("尝试启动服务，端口：" + port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            logs.add("服务器启动成功");
            while (true) {
                Socket client = serverSocket.accept();
                logs.add("新客户端连接: " + client);
                new Thread(new ClientHandler(client, users, logs)).start();
            }
        } catch (IOException e) {
            logs.add("服务器启动失败: " + e.getMessage());
        }
    }

    static class User {
        String username;
        Socket socket;

        public User(String username, Socket socket) {
            this.username = username;
            this.socket = socket;
        }
    }


    public class DB {
        private static final String URL = "jdbc:mysql://localhost:3306/ChatDB?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC";
        private static String USER;
        private static String PASSWORD;
        private static Connection conn;
        private static boolean connected = false;

        static {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                System.err.println("找不到 MySQL 驱动：" + e.getMessage());
            }
        }

        public static boolean connect(String user, String password) {
            try {
                USER = user;
                PASSWORD = password;
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                connected = true;
                System.out.println("数据库连接成功");
                return true;
            } catch (SQLException e) {
                System.err.println("数据库连接失败: " + e.getMessage());
                connected = false;
                return false;
            }
        }

        private static void ensureConnected() throws SQLException {
            if (conn == null || conn.isClosed() || !conn.isValid(2)) {
                reconnect();
            }
        }

        private static void reconnect() throws SQLException {
            if (USER == null || PASSWORD == null) throw new SQLException("未设置数据库凭据，无法重连");
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            connected = true;
        }

        public static void disconnect() {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                    connected = false;
                    System.out.println("数据库连接已关闭");
                }
            } catch (SQLException e) {
                System.err.println("断开数据库连接失败: " + e.getMessage());
            }
        }
        // 插入离线消息
        public static void insertOfflineMessage(String sender, String receiver, String message, long timeMillis) throws SQLException {
            ensureConnected();
            String sql = "INSERT INTO OfflineMessages (sender, receiver, message, send_time_millis) VALUES (?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, sender);
                stmt.setString(2, receiver);
                stmt.setString(3, message);
                stmt.setLong(4, timeMillis);
                stmt.executeUpdate();
            }
        }
        public static void insertOfflineMessage(String sender, String receiver, String message) throws SQLException {
            insertOfflineMessage(sender, receiver, message, System.currentTimeMillis());
        }


        // 获取并删除某个用户的所有离线消息
        public static List<JsonObject> getAndDeleteOfflineMessages(String username) throws SQLException {
            ensureConnected();
            String sql = "SELECT sender, message, send_time_millis FROM OfflineMessages WHERE receiver=?";
            List<JsonObject> list = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    JsonObject json = new JsonObject();
                    json.addProperty("type", "offlineMessage");
                    json.addProperty("sender", rs.getString("sender"));
                    json.addProperty("message", rs.getString("message"));
                    json.addProperty("target", username);
                    json.addProperty("time", rs.getLong("send_time_millis")); // 时间戳字段
                    list.add(json);
                }
            }

            // 删除这些消息
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM OfflineMessages WHERE receiver=?")) {
                stmt.setString(1, username);
                stmt.executeUpdate();
            }

            return list;
        }


        public static boolean userExists(String username) throws SQLException {
            ensureConnected();
            String sql = "SELECT COUNT(*) FROM Users WHERE username=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                ResultSet rs = stmt.executeQuery();
                rs.next();
                return rs.getInt(1) > 0;
            }
        }

        public static boolean checkPassword(String username, String passwd) throws SQLException {
            ensureConnected();
            String sql = "SELECT passwd FROM Users WHERE username=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    return rs.getString(1).equals(passwd);
                }
                return false;
            }
        }

        public static void createUser(String username, String passwd) throws SQLException {
            ensureConnected();
            String sql = "INSERT INTO Users (username, passwd) VALUES (?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                stmt.setString(2, passwd);
                stmt.executeUpdate();
            }
        }

        public static void deleteUser(String username) throws SQLException {
            ensureConnected();
            String sql = "DELETE FROM Users WHERE username=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, username);
                stmt.executeUpdate();
            }
        }

        public static void insertLog(String time, String content) {
            String sql = "INSERT INTO Logs (log_time, message) VALUES (?, ?)";
            try {
                ensureConnected();
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, time);
                    stmt.setString(2, content);
                    stmt.executeUpdate();
                }
            } catch (SQLException e) {
                System.err.println("日志写入失败: " + e.getMessage());
            }
        }

        public static void insertLog(String content) {
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
            insertLog(time, content);
        }

        // 添加联系人（禁止自己、禁止重复、目标用户必须存在）
        public static void addContact(String owner, String contact) throws SQLException {
            ensureConnected();

            if (owner.equals(contact)) {
                throw new SQLException("不能添加自己为联系人");
            }

            if (!userExists(contact)) {
                throw new SQLException("联系人用户不存在");
            }

            if (isContactExists(owner, contact)) {
                throw new SQLException("该联系人已存在");
            }

            String sql = "INSERT INTO Contacts (owner, contact) VALUES (?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, contact);
                stmt.executeUpdate();
            }
        }

        // 判断联系人是否已存在
        public static boolean isContactExists(String owner, String contact) throws SQLException {
            ensureConnected();
            String sql = "SELECT 1 FROM Contacts WHERE owner=? AND contact=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, contact);
                ResultSet rs = stmt.executeQuery();
                return rs.next(); // 存在一条即为 true
            }
        }

        // 删除联系人
        public static void removeContact(String owner, String contact) throws SQLException {
            ensureConnected();
            String sql = "DELETE FROM Contacts WHERE owner=? AND contact=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, owner);
                stmt.setString(2, contact);
                stmt.executeUpdate();
            }
        }

        // 获取联系人列表
        public static List<String> getContacts(String owner) throws SQLException {
            ensureConnected();
            String sql = "SELECT contact FROM Contacts WHERE owner=?";
            List<String> list = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, owner);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
            }
            return list;
        }

        public static void updateRemark(String owner, String contact, String remark) throws SQLException {
            ensureConnected();

            String sql = "UPDATE Contacts SET remark=? WHERE owner=? AND contact=?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, remark);
                stmt.setString(2, owner);
                stmt.setString(3, contact);
                int rows = stmt.executeUpdate();
                if (rows == 0) {
                    throw new SQLException("联系人不存在，无法修改备注");
                }
            }
        }

    }


    static class Chat {
        private final List<User> users;
        private final Lock lock = new ReentrantLock();

        // 定义全局离线消息缓存
        //private static final Map<String, List<JsonObject>> offlineMessages = new ConcurrentHashMap<>();
        public Chat(String username, String passwd, Socket socket, List<User> users) throws Exception {
            this.users = users;

            if (!isOnline(username, socket)) {
                login(username, passwd, socket);
            }
        }

        public Chat(Socket socket, List<User> users) {
            this.users = users;

        }

        public static void createAccount(String username, String passwd) throws Exception {
            if (DB.userExists(username)) {
                throw new RuntimeException("该账户已存在");
            }
            DB.createUser(username, passwd);
        }

        public static void deleteAccount(String username, String passwd) throws Exception {
            if (!DB.userExists(username)) {
                throw new RuntimeException("账户不存在");
            }
            if (!DB.checkPassword(username, passwd)) {
                throw new RuntimeException("账号或密码错误");
            }
            DB.deleteUser(username);
        }

        public void login(String username, String passwd, Socket socket) throws Exception {
            if (!DB.userExists(username)) {
                throw new RuntimeException("账户不存在");
            }
            if (!DB.checkPassword(username, passwd)) {
                throw new RuntimeException("账号或密码错误");
            }
            lock.lock();
            try {
                users.add(new User(username, socket));
            } finally {
                lock.unlock();
            }
        }

        public void logout(String username) {
            lock.lock();
            try {
                users.removeIf(u -> u.username.equals(username));
            } catch (Exception e){
                throw new RuntimeException("登出失败");
            }
            finally {
                lock.unlock();
            }
        }

        public void logout(Socket socket) {
            lock.lock();
            try {
                users.removeIf(u -> u.socket == socket);
            } catch(Exception e){
                throw new RuntimeException("登出失败");
            }
            finally {
                lock.unlock();
            }
        }

        public boolean isOnline(String username, Socket socket) {
            lock.lock();
            try {
                return users.stream().anyMatch(u -> u.username.equals(username) && u.socket == socket);
            } finally {
                lock.unlock();
            }
        }

        public int getOnlineCount() {
            lock.lock();
            try {
                return users.size();
            } finally {
                lock.unlock();
            }
        }

        public void sendToUser(String target, String message, String sender) {
            lock.lock();
            try {
                for (User u : users) {
                    if (u.username.equals(target)) {
                        OutputStream os = u.socket.getOutputStream();
                        JsonObject jsonObject = new JsonObject();
                        jsonObject.addProperty("type", "chat");
                        jsonObject.addProperty("sender", sender);
                        jsonObject.addProperty("message", message);
                        jsonObject.addProperty("target", target);
                        jsonObject.addProperty("time",System.currentTimeMillis());
                        os.write((jsonObject.toString() + "\n").getBytes(StandardCharsets.UTF_8));
                        os.flush();
                        return;
                    }
                }

                // 目标用户不在线，写入数据库
                DB.insertOfflineMessage(sender, target, message);


            } catch (Exception e) {
                throw new RuntimeException("已存储离线消息到数据库：from"  + sender + "to" + target);
            } finally {
                lock.unlock();
            }
        }


        public void sendToOther(String sender, String message) {
            lock.lock();
            try {
                for (User u : users) {
                    if (!u.username.equals(sender)) {
                        OutputStream os = u.socket.getOutputStream();
                        os.write(message.getBytes(StandardCharsets.UTF_8));
                        os.flush();
                    }
                }
            } catch (IOException ignored) {
            } finally {
                lock.unlock();
            }
        }
    }


    static class MessageProcessor {
        private final JsonObject parsed;
        private final JsonObject feedback = new JsonObject();
        private final Socket socket;
        private final List<User> users;
        private final List<String> logs;
        String raw = "";

        public MessageProcessor(String raw, Socket socket, List<User> users, List<String> logs) throws IOException {
            this.raw = raw;
            this.socket = socket;
            this.users = users;
            this.logs = logs;
            logs.add("收到来自["+socket+"]的消息："+raw);
            try {
                this.parsed = JsonParser.parseString(raw).getAsJsonObject();
            } catch (Exception e) {
                logs.add("解析错误：" + e);
                logs.add("无法解析的消息原文为："+raw);
                throw e;
            }
            handle();
        }

        private void respond(String status, String msgType) throws IOException {
            feedback.addProperty("type", "response");
            feedback.addProperty("status", status);
            feedback.addProperty("message", msgType);
            socket.getOutputStream().write((feedback.toString() + "\n").getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            logs.add("向"+socket+"发送消息："+feedback.toString());
        }
        private void respond(JsonObject msg){
            try {
                socket.getOutputStream().write((msg.toString() + "\n").getBytes(StandardCharsets.UTF_8));
                socket.getOutputStream().flush();
                logs.add("向"+socket+"发送消息："+msg.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void handle() throws IOException {
            String type = parsed.has("type") ? parsed.get("type").getAsString() : "";
            String username = parsed.has("username") ? parsed.get("username").getAsString() : "";
            String passwd = parsed.has("passwd") ? parsed.get("passwd").getAsString() : "";
            String target = parsed.has("target") ? parsed.get("target").getAsString() : "";
            String message = parsed.has("message") ? parsed.get("message").getAsString() : "";

            try {
                switch (type) {
                    case "login" -> {
                        new Chat(username, passwd, socket, users);
                        logs.add(username + "登陆成功");
                        respond("ok", "login");
                        // 登录成功，发送离线消息
                        List<JsonObject> offlineMsgs = DB.getAndDeleteOfflineMessages(username);
                        for (JsonObject msg : offlineMsgs) {
                            respond(msg);
                        }
                        logs.add("推送离线消息 " + offlineMsgs.size() + " 条给 " + username);

                    }

                    case "create" -> {
                        Chat.createAccount(username, passwd);
                        logs.add("新用户创建：" + username);
                        respond("ok", "create");
                    }

                    case "delete" -> {
                        Chat.deleteAccount(username, passwd);
                        JsonObject result=new JsonObject();
                        result.addProperty("type", "delete");
                        result.addProperty("status", "ok");
                        result.addProperty("message","delete");
                        respond(result);
                        logs.add("用户注销：" + username);
                    }

                    case "logout" -> {
                        new Chat(username, passwd, socket, users).logout(username);
                        logs.add("用户登出：" + username);
                        respond("ok", "logout");

                    }

                    case "chat" -> {
                        new Chat(username, passwd, socket, users).sendToUser(target, message, username);
                        logs.add("用户" + username + "发送消息给" + target + "：" + message);
                        respond("ok", "chat");
                    }

                    case "group" -> {
                        new Chat(username, passwd, socket, users).sendToOther(username, message);
                        logs.add("用户" + username + "群发消息：" + message);
                        respond("ok", "group");
                    }


                    case "add_friend" -> {
                        try {
                            DB.addContact(username, target);
                            JsonObject result=new JsonObject();
                            logs.add(username + " 添加联系人：" + target);
                            result.addProperty("type", "add_friend");
                            result.addProperty("status", "ok");
                            result.addProperty("message", target);
                            respond(result);

                        } catch (Exception e) {
                            logs.add("添加联系人失败：" + e.getMessage());
                            JsonObject result=new JsonObject();
                            result.addProperty("type", "add_friend");
                            result.addProperty("status", "error");
                            result.addProperty("message", e.getMessage());
                            respond(result);
                        }
                    }

                    case "remove_friend" -> {
                        try {
                            DB.removeContact(username, target);
                            logs.add(username + " 删除联系人：" + target);
                            JsonObject result=new JsonObject();
                            result.addProperty("type", "remove_friend");
                            result.addProperty("status", "ok");
                            result.addProperty("message", target);
                            respond(result);
                        } catch (Exception e) {
                            logs.add("删除联系人失败：" + e.getMessage());
                            JsonObject result=new JsonObject();
                            result.addProperty("type", "remove_friend");
                            result.addProperty("status", "error");
                            result.addProperty("message", e.getMessage());
                            respond(result);
                        }
                    }

                    case "list_friend" -> {
                        try {
                            List<String> list = DB.getContacts(username);
                            JsonArray array = new JsonArray();
                            for (String contact : list) {
                                JsonObject c = new JsonObject();
                                c.addProperty("username", contact);
                                c.addProperty("remark", ""); // 如有 remark，可一起查出加入
                                array.add(c);
                            }
                            JsonObject result = new JsonObject();
                            result.addProperty("type", "list_friend");
                            result.addProperty("status", "ok");
                            result.addProperty("message", "list_friend");
                            result.add("friends", array);
                            respond(result);
                        } catch (Exception e) {
                            logs.add("获取联系人失败：" + e.getMessage());
                            respond("error", "list_friend");
                        }
                    }

                    case "remark_friend" -> {
                        String remark = parsed.has("remark") ? parsed.get("remark").getAsString() : "";
                        try {
                            DB.updateRemark(username, target, remark);
                            logs.add(username + " 更新联系人备注：" + target + " => " + remark);
                            respond("ok", "remark_friend");
                        } catch (Exception e) {
                            logs.add("更新备注失败：" + e.getMessage());
                            respond("error", "remark_friend");
                        }
                    }
                    case "heartbeat" -> {
                        logs.add(socket.toString() + "仍在心跳");
                        JsonObject result=new JsonObject();
                        result.addProperty("type", "heartbeat");
                        result.addProperty("status", "ok");
                        result.addProperty("message", socket.toString());
                        respond(result);
                    }


                    default -> {
                        System.out.println(raw);
                        logs.add("未知消息类型: " + type);
                        //respond("error", "unknown");
                    }
                }


            } catch (Exception e) {
                logs.add("处理失败: " + e.getMessage());
                respond("error", type);
            }
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;
        private final List<User> users;
        private final List<String> logs;

        public ClientHandler(Socket socket, List<User> users, List<String> logs) {
            this.socket = socket;
            this.users = users;
            this.logs = logs;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    new MessageProcessor(line, socket, users, logs);
                }
            } catch (IOException e) {
                logs.add("客户端异常断开: " + e.getMessage());
                new Chat(socket, users).logout(socket);
            }
        }
    }


    // 日志持久化入数据库
    static class Logger {
        public static void monitor(List<String> logs) {
            new Thread(() -> {
                int lastSize = 0;
                while (true) {
                    try {
                        while (lastSize < logs.size()) {
                            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
                            String content = logs.get(lastSize);
                            System.out.println("[" + time + "]" + content);
                            DB.insertLog(time, content);
                            lastSize++;
                        }
                    } catch (Exception ignored) {
                    }
                }
            }).start();
        }
    }
}