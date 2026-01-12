import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Proxy {

    private static class ProxyNode {
        String host;
        Integer port;
        Protocol protocol;
        boolean isProxy;
        boolean hasTCP;
        boolean hasUDP;
        boolean reachable;
        long nextRetryAtMs;
        int failCount;

        ProxyNode(String host, Integer port) {
            this.host = host;
            this.port = port;
            this.protocol = null;
            this.isProxy = false;
            this.hasTCP = false;
            this.hasUDP = false;
            this.reachable = true;
            this.nextRetryAtMs = 0;
            this.failCount = 0;
        }

        String key() {
            return host + ":" + port;
        }

    }

    public static long timeNow() {
        return System.currentTimeMillis();
    }

    // delay to retry
    private static final long[] delay = {1000, 2000, 4000, 8000, 16000, 30000};

    public static long retryDelayMs(ProxyNode node) {
        int k = Math.min(Math.max(node.failCount, 0), 5);
        long d = delay[k];
        if (node.isProxy) d = Math.min(d, 4000);
        return d;
    }

    public static void markUnreachable(ProxyNode node) {
        node.reachable = false;
        node.failCount++;
        node.nextRetryAtMs = timeNow() + retryDelayMs(node);
    }

    public static void markReachable(ProxyNode node) {
        node.reachable = true;
        node.failCount = 0;
        node.nextRetryAtMs = 0L;
    }

    private enum Protocol {
        TCP,
        UDP
    }


    private static List<ProxyNode> nodes = new ArrayList<>();

    // to reach the appropriate server taking care of the given key
    private static volatile Map<String, ProxyNode> nodeMap = new ConcurrentHashMap<>();
    private final int listenPort;

    public Proxy(int listenPort, List<String> hosts, List<Integer> ports) {
        this.listenPort = listenPort;
        for (int i = 0; i < hosts.size(); i++) {
            nodes.add(new ProxyNode(hosts.get(i), ports.get(i)));
        }
    }


    public static void main(String[] args) throws IOException {
        // java Proxy -port 2106 -server localhost 3107

        int port = 0;
        List<String> hosts = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();

        // java Proxy -port 2106 -server localhost 3107 -server localhost 3108
        //  0     1     2     3     4         5      6     7        8       9
        for (int i = 0; i < args.length; ) {
            switch (args[i]) {
                case "-port":
                    if (i + 1 >= args.length) {
                        System.err.println("Missing value after -port");
                        System.exit(1);
                    }

                    try {
                        port = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid port number " + args[i + 1]);
                        System.exit(1);
                    }
                    i += 2;
                    break;
                case "-server":
                    if (i + 2 >= args.length) {
                        System.err.println("Missing <address> <port> after -server");
                        System.exit(1);
                    }
                    hosts.add(args[i + 1]);

                    try {
                        ports.add(Integer.valueOf(args[i + 2]));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid port number for server" + args[i + 2]);
                        System.exit(1);
                    }

                    i += 3;
                    break;
                default:
                    System.err.println("Unknown parameter: " + args[i]);
                    System.err.println("Incorrect execution syntax, try using:");
                    System.err.println("java Proxy -port <port> -server <address> <port>");
                    System.exit(1);
            }
        }

        if (port == 0 || hosts.isEmpty() || ports.isEmpty()) {
            System.err.println("Incorrect execution syntax, try using:");
            System.err.println("java Proxy -port <port> -server <address> <port>");
            System.exit(1);
        }

        Proxy proxy = new Proxy(port, hosts, ports);

        proxy.init();
        proxy.start();
    }

    private void start() {
        // java Proxy -port 2106 -server localhost 3107
        // for TCP
        Thread thread = new Thread(() -> {
            try (ServerSocket socket = new ServerSocket(listenPort);) {
                while (true) {

                    Socket clientSocket = socket.accept();
                    System.out.println("New client socket connected");
                    Thread threadT = new Thread(() -> handleTCPClient(clientSocket));
                    threadT.start();
                }

            } catch (IOException e) {
                System.err.println("Proxy TCP error: " + e);
                System.exit(1);
            }
        });
        thread.start();

        // java Proxy -port 2106 -udpserver localhost 3107
        // for UDP
        Thread threadT = new Thread(() -> {
            try (DatagramSocket socket = new DatagramSocket(listenPort);) {

                System.out.println("New datagram socket connected");
                handleUDPClient(socket);

            } catch (IOException e) {
                System.err.println("Proxy UDP error: " + e);
                System.exit(1);
            }
        });
        threadT.start();

        Thread refresher = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(3000);
                    refreshKeys();
                } catch (InterruptedException ignored) {
                }
            }
        });
        refresher.setDaemon(true);
        refresher.start();
    }

    private void init() {

        // figuring out what this node will be - proxy or server
        for (ProxyNode node : nodes) {

            if (!node.reachable && timeNow() < node.nextRetryAtMs) {
                continue;
            }

            boolean hasTcp = ifTCPExists(node.host, node.port);
            boolean hasUdp = ifUDPExists(node.host, node.port);
            node.hasTCP = hasTcp;
            node.hasUDP = hasUdp;
            if (!hasTcp && !hasUdp) {
                System.out.println("No TCP or UDP response : " + node.host + ":" + node.port);
                node.protocol = null;
                node.isProxy = false;
                markUnreachable(node);
                continue;
            }
            markReachable(node);
            // "After starting a proxy opens communication ports (both TCP and UDP)"
            // so if it has BOTH tcp and udp - it is proxy
            if (hasTcp && hasUdp) {
                node.isProxy = true;
                // doesn't really matter
                node.protocol = Protocol.TCP;
                System.out.println("This node is proxy - " + node.host + ":" + node.port);
            } else {
                node.isProxy = false;
                if (hasTcp) {
                    node.protocol = Protocol.TCP;
                } else node.protocol = Protocol.UDP;

                System.out.println("This node is " + node.protocol + " server - " + node.host + ":" + node.port);
            }
        }

        refreshKeys();
    }

    private static boolean ifTCPExists(String host, int port) {
        String response = sendToTCPServer(host, port, "GET NAMES");
        return response != null && response.startsWith("OK");
    }

    private static boolean ifUDPExists(String host, int port) {
        String response = sendToUDPServer(host, port, "GET NAMES");
        return response != null && response.startsWith("OK");
    }

    private synchronized void refreshKeys() {
        Map<String, ProxyNode> updated = new ConcurrentHashMap<>();

        // gathering all keys and remembering routes to them
        for (ProxyNode node : nodes) {

            // skipping nodes thhat are unreachable and it's still not time to retry
            if (!node.reachable && timeNow() < node.nextRetryAtMs) {
                continue;
            }

            String response;

            if (node.protocol == Protocol.TCP) {
                response = sendToTCPServer(node.host, node.port, "GET NAMES");
            } else if (node.protocol == Protocol.UDP) {
                response = sendToUDPServer(node.host, node.port, "GET NAMES");
            } else {
                boolean hasTcp = ifTCPExists(node.host, node.port);
                boolean hasUdp = ifUDPExists(node.host, node.port);
                node.hasTCP = hasTcp;
                node.hasUDP = hasUdp;
                if (!hasTcp && !hasUdp) {
                    markUnreachable(node);
                    continue;
                }

                markReachable(node);
                if (hasTcp && hasUdp) {
                    node.isProxy = true;
                }
                node.protocol = hasTcp ? Protocol.TCP : Protocol.UDP;

                if (node.protocol == Protocol.TCP) {
                    response = sendToTCPServer(node.host, node.port, "GET NAMES");
                } else {
                    response = sendToUDPServer(node.host, node.port, "GET NAMES");
                }
            }

            if (response == null) {
                markUnreachable(node);
                continue;
            }

            response = response.trim();
            if (response.isEmpty()) {
                markUnreachable(node);
                continue;
            }

            if (response.equals("NA")) {
                // it means that our node did gave a responce, just didn't give keys,
                // so we can leave it
                markReachable(node);
                System.err.println("node " + node.key() + " returned NA for GET NAMES");
                continue;
            }
            String[] segments = response.split("\\s+");

            // OK <number> [name]+
            // OK 3 a b c
            if (segments.length < 3 || !segments[0].equals("OK")) {
                continue;
            }


            try {
                int num = Integer.parseInt(segments[1]);
                if (segments.length - 2 != num) {
                    System.err.println("length is not in accordance with amount of args " + segments[1]);
                    continue;
                }
            } catch (NumberFormatException e) {
                System.err.println("Invalid number " + segments[1]);
                continue;
            }

            for (int i = 2; i < segments.length; i++) {
                String key = segments[i];
                // avoiding duplicates
                updated.putIfAbsent(key, node);
            }
        }
        nodeMap = updated;
    }


    private static String readLine(InputStream iStream) throws IOException {
        int b;
        StringBuilder sb = new StringBuilder();
        while ((b = iStream.read()) != -1) {
            char c = (char) b;
            if (c == '\n') {
                break;
            }
            if (c == '\r') {
                continue;
            }
            sb.append(c);
        }
        //if we did not read anything and it already closed
        if (sb.length() == 0 && b == -1) return null;
        else return sb.toString();
    }

    private static String sendToTCPServer(String host, int port, String command) {

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1200);
            socket.setSoTimeout(1200);
            OutputStream ostream = socket.getOutputStream();
            InputStream istream = socket.getInputStream();


            ostream.write((command + "\n").getBytes());
            ostream.flush();

            String response = readLine(istream);
            if (response == null) return null;
            response = response.trim();
            return response.isEmpty() ? null : response;

        } catch (IOException e) {
            return null;
        }
    }

    private static String sendToUDPServer(String host, int port, String command) {

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(1200);

            String message = command + "\n";
            byte[] data = message.getBytes();

            DatagramPacket packetSend = new DatagramPacket(data, data.length, InetAddress.getByName(host), port);
            socket.send(packetSend);


            byte[] buffer = new byte[1024];

            DatagramPacket packetReceive = new DatagramPacket(buffer, buffer.length);
            socket.receive(packetReceive);


            String response = new String(packetReceive.getData(), 0, packetReceive.getLength());
            response = response.trim();
            return response.isEmpty() ? null : response;

        } catch (IOException e) {
            return null;
        }
    }

    /*
    Cleint sends GET NAMES
    send get names request to server
    server responds OK <number> [name]+
    send responce back to client
     */
    private String getNames() {

        // does not allow duplicates, saves adding order
        Set<String> names = new LinkedHashSet<>(nodeMap.keySet());

        if (names.isEmpty()) {
            return "NA";
        }

        // form responce to a client
        // OK <number> [name]+
        StringBuilder sb = new StringBuilder();
        sb.append("OK ");
        sb.append(names.size());
        for (String name : names) {
            sb.append(" ");
            sb.append(name);
        }
        return sb.toString();
    }

    /*

    clien sends GET VALUE <name>
    server responds OK <value>
    or NA
    */
    private String getValue(String name) {

        ProxyNode node = nodeMap.get(name);
        String responce = "NA";

        // if we don't know key - try to refresh routes and check again
        if (node == null) {
            refreshKeys();
            node = nodeMap.get(name);
            if (node == null) {
                return "NA";
            }
        }

        String serverResponse;
        if (node.protocol == Protocol.TCP) {
            serverResponse = sendToTCPServer(node.host, node.port, "GET VALUE " + name);
        } else {
            serverResponse = sendToUDPServer(node.host, node.port, "GET VALUE " + name);
        }
        // OK 1
        if (serverResponse == null) {
            markUnreachable(node);
            refreshKeys();
            return "NA";
        }
        serverResponse = serverResponse.trim();

        if (serverResponse.startsWith("OK")) {
            return serverResponse;
        }
        return "NA";
    }

    /*
    client sends request SET <name> <value>
    server responds OK
    or NA

    SET a 1
    */
    private String setNameValue(String name, String value) {

        ProxyNode node = nodeMap.get(name);
        String responce = "NA";

        // if we don't know key - try to refresh routes and check again
        if (node == null) {
            refreshKeys();
            node = nodeMap.get(name);
            if (node == null) {
                return "NA";
            }
        }

        String serverResponse;
        if (node.protocol == Protocol.TCP) {
            serverResponse = sendToTCPServer(node.host, node.port, "SET " + name + " " + value);
        } else {
            serverResponse = sendToUDPServer(node.host, node.port, "SET " + name + " " + value);
        }
        if (serverResponse == null) {
            markUnreachable(node);
            refreshKeys();
            return "NA";
        }

        serverResponse = serverResponse.trim();
        if (serverResponse.equals("OK")) {
            return serverResponse;
        }
        return "NA";
    }

    /*
    client sends request QUIT
    no responce
     */
    private void quit() {

        for (ProxyNode node : nodes) {
            if (node.protocol == null) {
                continue;
            }

            if (node.protocol == Protocol.TCP) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(node.host, node.port), 1200);
                    socket.setSoTimeout(1200);
                    OutputStream ostream = socket.getOutputStream();

                    ostream.write("QUIT\n".getBytes());
                    ostream.flush();
                } catch (IOException e) {
                    System.err.println("quit error sending request to TCP server " + node.host + ":" + node.port);
                }
            } else {
                try (DatagramSocket socket = new DatagramSocket()) {
                    socket.setSoTimeout(1200);
                    byte[] data = "QUIT\n".getBytes();
                    DatagramPacket packetSend = new DatagramPacket(data, data.length, InetAddress.getByName(node.host), node.port);
                    socket.send(packetSend);

                } catch (IOException e) {
                    System.err.println("quit error sending request to UDP server " + node.host + ":" + node.port);
                }
            }
        }
    }

    public static String newRequestId() {
        return System.currentTimeMillis() + "-" + java.util.UUID.randomUUID();
    }

    private String handleCommand(String command) {
        if (command == null) return "NA";

        command = command.trim();
        if (command.isEmpty()) return "NA";

        command = command.trim();
        String[] segments = command.split("\\s+");

        // PRX GETNAMES <requestID> <countdown>
        // PRX GETVALUE <requestID> <countdown> <name>
        // PRX SET <requestID> <countdown> <name> <value>
        if (segments.length > 0 && segments[0].equals("PRX")) {
            if (segments.length == 4 && segments[1].equals("GETNAMES")) {
                try {
                    return this.proxyGetNames(segments[2], Integer.parseInt(segments[3]));
                } catch (Exception e) {
                    return "NA";
                }
            } else if (segments.length == 5 && segments[1].equals("GETVALUE")) {
                try {
                    return this.proxyGetValue(segments[2], Integer.parseInt(segments[3]), segments[4]);
                } catch (Exception e) {
                    return "NA";
                }
            } else if (segments.length == 6 && segments[1].equals("SET")) {
                try {
                    return this.proxySetNameValue(segments[2], Integer.parseInt(segments[3]), segments[4], segments[5]);
                } catch (Exception e) {
                    return "NA";
                }
            }
            return "NA";
        }


        // GET NAMES
        // GET VALUE <name>
        // SET <name> <value>
        // QUIT
        if (segments.length == 1) {
            if (segments[0].equals("QUIT")) {
                this.quit();
                return "QUIT";
            } else return "NA";

        } else if (segments.length == 2) {
            if (segments[0].equals("GET") && segments[1].equals("NAMES")) {
                return proxyGetNames(newRequestId(), 5);

            } else return "NA";
        } else if (segments.length == 3) {
            if (segments[0].equals("GET") && segments[1].equals("VALUE")) {
                return proxyGetValue(newRequestId(), 5, segments[2]);
            } else if (segments[0].equals("SET")) {
                return proxySetNameValue(newRequestId(), 5, segments[1], segments[2]);
            } else return "NA";

        } else return "NA";

    }

    private void handleTCPClient(Socket clientSocket) {

        try (Socket socket = clientSocket) {
            OutputStream ostream = socket.getOutputStream();
            InputStream istream = socket.getInputStream();

            String line = readLine(istream);

            if (line == null) {
                return;
            }
            line = line.trim();
            if (line.isEmpty()) return;

            String trimmed = line.trim();
            boolean internal = trimmed.startsWith("PRX ");
            if (!internal) {
                System.out.println(trimmed);
            }

            String response = this.handleCommand(line);

            if (response != null && !response.equals("QUIT")) {
                ostream.write((response + "\n").getBytes());
                ostream.flush();
                System.out.println(response);
            }
            if (line.equals("QUIT")) {
                System.out.println("Proxy TCP QUIT");
                System.exit(0);
            }
        } catch (IOException e) {
            System.err.println("handleTCPClient error");
        }
    }

    private void handleUDPClient(DatagramSocket socket) {
        try {
            while (true) {
                byte[] receiveBuffer = new byte[1024];

                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                socket.receive(receivePacket);

                String message = new String(receivePacket.getData(), 0, receivePacket.getLength());

                String response = handleCommand(message);

                if (response != null && !response.equals("QUIT")) {
                    byte[] responseBuffer = (response + "\n").getBytes();
                    DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length, receivePacket.getAddress(), receivePacket.getPort());
                    socket.send(responsePacket);
                }
                if (response != null && response.equals("QUIT")) {
                    socket.close();
                    System.out.println("Proxy UDP QUIT");
                    System.exit(0);
                }


            }
        } catch (IOException e) {
            System.err.println("handleUDPClient error");
        } finally {
            socket.close();
        }
    }


    // proxy -  proxy


    // for anti cycles occuring
    private static final ConcurrentHashMap<String, Long> seenRequest = new ConcurrentHashMap<>();


    private static boolean isNewRequest(String reqId) {
        long now = timeNow();
        long expireAt = now + 60000;
        Long a = seenRequest.putIfAbsent(reqId, expireAt);
        if (seenRequest.size() > 5000) {
            for (Map.Entry<String, Long> e : seenRequest.entrySet()) {
                if (e.getValue() < now) seenRequest.remove(e.getKey(), e.getValue());
            }
        }
        return a == null;
    }

    /*
    Cleint sends PRX GETNAMES
    send get names request to server
    server responds OK <number> [name]+
    send responce back to client
     */
    private String proxyGetNames(String requestID, int countdown) {
        if (requestID == null || requestID.isEmpty()) return "NA";
        if (countdown < 0) return "NA";
        if (!isNewRequest("GETNAMES:" + requestID)) return "OK 0";


        // does not allow duplicates, saves adding order
        Set<String> names = new LinkedHashSet<>(nodeMap.keySet());

        //if our countown allows us to - we will be asking other proxies
        int nextCount = countdown - 1;

        if (nextCount >= 0) {
            for (ProxyNode node : nodes) {
                if (!node.isProxy) continue;
                if (!node.reachable && timeNow() < node.nextRetryAtMs) continue;

                String comm = "PRX GETNAMES " + requestID + " " + nextCount;
                String response;
                if (node.protocol == Protocol.TCP) response = sendToTCPServer(node.host, node.port, comm);
                else if (node.protocol == Protocol.UDP) response = sendToUDPServer(node.host, node.port, comm);
                else continue;


                if (response == null) {
                    markUnreachable(node);
                    continue;
                }
                response = response.trim();
                markReachable(node);

                if (response.equals("NA") || response.isEmpty()) continue;


                String[] segments = response.split("\\s+");

                // OK <number> [name]+
                // OK 3 a b c
                if (segments.length < 3 || !segments[0].equals("OK")) {
                    continue;
                }

                try {
                    int num = Integer.parseInt(segments[1]);
                    if (segments.length - 2 != num) {
                        System.err.println("length is not in accordance with amount of args " + segments[1]);
                        continue;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number " + segments[1]);
                    continue;
                }

                for (int i = 2; i < segments.length; i++) {
                    names.add(segments[i]);
                }

            }
        }
        if (names.isEmpty()) return "NA";
        // OK <number> [name]+
        StringBuilder sb = new StringBuilder();
        sb.append("OK ");
        sb.append(names.size());
        for (String name : names) {
            sb.append(" ");
            sb.append(name);
        }
        return sb.toString();
    }

    private String forwardGetValue(ProxyNode node, String requestID, int countdown, String name) {
        if (node == null || node.protocol == null) return null;
        if (countdown < 0) return null;
        String comm;
        if (node.isProxy) {
            comm = "PRX GETVALUE " + requestID + " " + countdown + " " + name;
        } else {
            comm = "GET VALUE " + name;
        }
        if (node.protocol == Protocol.TCP) {
            return sendToTCPServer(node.host, node.port, comm);
        } else {
            return sendToUDPServer(node.host, node.port, comm);
        }
    }

    /*

    clien sends GET VALUE <name>
    server responds OK <value>
    or NA
    */
    private String proxyGetValue(String requestID, int countdown, String name) {
        if (requestID == null || requestID.isEmpty()) return "NA";
        if (name == null || name.isEmpty()) return "NA";
        if (countdown < 0) return "NA";
        if (!isNewRequest("GETVALUE:" + requestID)) return "NA";

        ProxyNode tryy = nodeMap.get(name);
        if (tryy != null) {
            int next = countdown - 1;
            if (next < 0) next = 0;
            String resp = forwardGetValue(tryy, requestID, next, name);
            if (tryy.isProxy && (resp == null || resp.trim().equals("NA"))) {
                refreshKeys();
                resp = forwardGetValue(tryy, requestID, next, name);
            }
            if (resp != null && resp.trim().startsWith("OK")) return resp.trim();

        }


        int nextCount = countdown - 1;
        if (nextCount < 0) return "NA";

        for (ProxyNode node : nodes) {
            if (!node.isProxy) continue;
            if (!node.reachable && timeNow() < node.nextRetryAtMs) {
                continue;
            }

            String comm = "PRX GETVALUE " + requestID + " " + nextCount + " " + name;

            String response;
            if (node.protocol == Protocol.TCP) response = sendToTCPServer(node.host, node.port, comm);
            else if (node.protocol == Protocol.UDP) response = sendToUDPServer(node.host, node.port, comm);
            else continue;


            if (response == null) {
                markUnreachable(node);
                continue;
            }
            response = response.trim();
            markReachable(node);

            if (response.startsWith("OK")) {
                return response;
            }
        }
        return "NA";
    }
    private String forwardSetValue(ProxyNode node, String requestID, int countdown, String name, String value) {
        if (node == null || node.protocol == null) return null;
        if (countdown < 0) return null;

        String comm;
        if (node.isProxy) {
            comm = "PRX SET " + requestID + " " + countdown + " " + name + " " + value;
        } else {
            comm = "SET " + name + " " + value;
        }

        if (node.protocol == Protocol.TCP) {
            return sendToTCPServer(node.host, node.port, comm);
        } else {
            return sendToUDPServer(node.host, node.port, comm);
        }
    }

    /*
    client sends request SET <name> <value>
    server responds OK
    or NA

    SET a 1
    */
    private String proxySetNameValue(String requestID, int countdown, String name, String value) {
        if (requestID == null || requestID.isEmpty()) return "NA";
        if (name == null || name.isEmpty()) return "NA";
        if (value == null || value.isEmpty()) return "NA";
        if (countdown < 0) return "NA";
        if (!isNewRequest("SET:" + requestID)) return "NA";

        // first we are trying locally
        ProxyNode tryy = nodeMap.get(name);
        if (tryy != null) {
            int next = countdown - 1;
            if (next < 0) next = 0;
            String resp = forwardSetValue(tryy, requestID, next, name, value);
            if (tryy.isProxy && (resp == null || resp.trim().equals("NA"))) {
                refreshKeys();
                resp = forwardSetValue(tryy, requestID, next, name, value);
            }
            if (resp != null && resp.trim().equals("OK")) return "OK";
        }

        // if not locally - then we are asking other proxies
        int nextCount = countdown - 1;
        if (nextCount < 0) return "NA";

        for (ProxyNode node : nodes) {
            if (!node.isProxy) continue;
            if (!node.reachable && timeNow() < node.nextRetryAtMs) {
                continue;
            }

            String comm = "PRX SET " + requestID + " " + nextCount + " " + name + " " + value;

            String response;
            if (node.protocol == Protocol.TCP) response = sendToTCPServer(node.host, node.port, comm);
            else if (node.protocol == Protocol.UDP) response = sendToUDPServer(node.host, node.port, comm);
            else continue;


            if (response == null) {
                markUnreachable(node);
                continue;
            }
            response = response.trim();
            //if server responded with NA its not a mistake, it just does not now the key
            markReachable(node);

            if (response.equals("OK")) {
                return response;
            }

        }
        return "NA";
    }

}

