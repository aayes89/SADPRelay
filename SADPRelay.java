/*
 * The MIT License
 *
 * Copyright 2025 Allan (Slam).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package sadprelay;

/**
 *
 * @author Slam
 */
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * SADPRelay - bridge UDP multicast <-> relay <-> remote unicast
 *
 * Usage: java SADPRelay mode [relayHost relayPort multicastGroup multicastPort
 * token]
 *
 * Modes: relay, transmitter, receiver
 *
 * Examples: Relay (listens on 12345): java SADPRelay relay 0.0.0.0 12345
 * 239.255.255.250 37020 mytoken Transmitter (on LAN side): java SADPRelay
 * transmitter <relay_ip> 12345 239.255.255.250 37020 mytoken Receiver (remote):
 * java SADPRelay receiver <relay_ip> 12345 239.255.255.250 37020 mytoken
 */
public class SADPRelay {

    private static final int BUFFER_SIZE = 65536;
    private static final long REGISTRY_EXPIRE_MS = 30_000L; // 30s
    private static final int QUERY_SOCKET_POOL = 4; // limit number of concurrent multicast sockets/threads
    private static final int TRANSMITTER_RESP_TIMEOUT_MS = 5000; // wait time for responses
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String DEF_RELAY_HOST = "0.0.0.0";
    private static final int DEF_RELAY_PORT = 12345;
    private static final String DEF_MCAST_GROUP = "239.255.255.250";
    private static final int DEF_MCAST_PORT = 37020;
    private static final String DEF_TOKEN = "";

    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("SADP Relay tunnel for Hikvision softwares\nBy Allan Ayes (Slam)\nVersion 0.0.1 (2025)\n\n");
            System.out.println("Usage: java SADPRelay mode [relayHost relayPort multicastGroup multicastPort token]");
            System.out.println("Modes: relay, transmitter, receiver\n\n");
            System.out.println("Relay: java SADPRelay relay 0.0.0.0 12345 239.255.255.250 37020 mytoken\n"
                    + "Receiver: java SADPRelay receiver <ip_relay> 12345 239.255.255.250 37020 mytoken\n"
                    + "Transmitter: java SADPRelay transmitter <ip_relay> 12345 239.255.255.250 37020 mytoken");
            System.out.println("Si necesita iptables en el receiver ejecute:\n"
                    + "sudo iptables -t nat -A OUTPUT -d 239.255.255.250 -p udp --dport 37020 -j DNAT --to-destination 127.0.0.1:37020\n");
            System.exit(1);

        }
       
        // defaults
        String mode = (args.length > 0) ? args[0] : "";
        String relayHost = DEF_RELAY_HOST;
        int relayPort = DEF_RELAY_PORT;
        String multicastGroup = DEF_MCAST_GROUP;
        int multicastPort = DEF_MCAST_PORT;
        String token = DEF_TOKEN;

        if (args.length > 1 && args[1] != null && !args[1].isEmpty()) {
            relayHost = args[1];
        }
        if (args.length > 2 && args[2] != null && !args[2].isEmpty()) {
            try {
                relayPort = Integer.parseInt(args[2]);
                if (relayPort <= 0 || relayPort > 0xFFFF) {
                    log("Invalid relayPort " + args[2] + " - using default " + DEF_RELAY_PORT);
                    relayPort = DEF_RELAY_PORT;
                }
            } catch (NumberFormatException e) {
                log("Invalid relayPort '" + args[2] + "' - using default " + DEF_RELAY_PORT);
                relayPort = DEF_RELAY_PORT;
            }
        }
        if (args.length > 3 && args[3] != null && !args[3].isEmpty()) {
            multicastGroup = args[3];
        }
        if (args.length > 4 && args[4] != null && !args[4].isEmpty()) {
            try {
                multicastPort = Integer.parseInt(args[4]);
                if (multicastPort <= 0 || multicastPort > 0xFFFF) {
                    log("Invalid multicastPort " + args[4] + " - using default " + DEF_MCAST_PORT);
                    multicastPort = DEF_MCAST_PORT;
                }
            } catch (NumberFormatException e) {
                log("Invalid multicastPort '" + args[4] + "' - using default " + DEF_MCAST_PORT);
                multicastPort = DEF_MCAST_PORT;
            }
        }
        if (args.length > 5 && args[5] != null) {
            token = args[5];
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            log("Shutdown requested");
        }));

        switch (mode) {
            case "relay":
                runRelay(relayHost, relayPort, token);
                break;
            case "transmitter":
                runTransmitter(relayHost, relayPort, multicastGroup, multicastPort, token);
                break;
            case "receiver":
                String os = System.getProperty("os.name").toLowerCase();
                if (os.contains("linux")) {
                    runReceiverLinux(relayHost, relayPort, multicastGroup, multicastPort, token);
                } else {
                    runReceiverWin(relayHost, relayPort, multicastGroup, multicastPort, token);
                }
                break;
            default:
                System.out.println("Invalid mode");
        }
    }

    /* ----------------------------
       Relay implementation
       ---------------------------- */
    private static void runRelay(String bindHost, int relayPort, String expectedToken) throws Exception {
        DatagramSocket socket = new DatagramSocket(null);
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(InetAddress.getByName(bindHost), relayPort));
        socket.setSoTimeout(2000); // short timeout to periodically purge registry/handle shutdown
        log("Relay running on " + bindHost + ":" + relayPort + " token-protected=" + (expectedToken != null));

        final Object regLock = new Object();
        final Holder<ClientInfo> transmitter = new Holder<>(null);
        final Holder<ClientInfo> receiver = new Holder<>(null);

        byte[] buf = new byte[BUFFER_SIZE];

        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                socket.receive(p);

                InetSocketAddress sender = new InetSocketAddress(p.getAddress(), p.getPort());
                int len = p.getLength();
                int off = p.getOffset();
                if (len <= 0) {
                    continue;
                }

                // Try to parse simple ASCII commands REGISTER/HEARTBEAT (max ~128 bytes)
                String msg = null;
                try {
                    msg = new String(p.getData(), off, Math.min(len, 256), "UTF-8");
                } catch (Exception ignored) {
                }

                if (msg != null && msg.startsWith("REGISTER")) {
                    // REGISTER role [token]
                    String[] parts = msg.split("\\s+");
                    String role = (parts.length >= 2) ? parts[1].trim() : "";
                    String providedToken = (parts.length >= 3) ? parts[2].trim() : null;

                    if (expectedToken != null && !expectedToken.equals(providedToken)) {
                        log("Reject REGISTER " + role + " from " + sender + " - invalid token");
                        continue;
                    }

                    synchronized (regLock) {
                        if ("transmitter".equals(role)) {
                            transmitter.value = new ClientInfo(sender);
                            log("Registered transmitter: " + sender);
                        } else if ("receiver".equals(role)) {
                            receiver.value = new ClientInfo(sender);
                            log("Registered receiver: " + sender);
                        } else {
                            log("Unknown REGISTER role from " + sender + ": " + role);
                        }
                    }
                    continue;
                } else if (msg != null && msg.startsWith("HEARTBEAT")) {
                    // HEARTBEAT role [token]
                    String[] parts = msg.split("\\s+");
                    String role = (parts.length >= 2) ? parts[1].trim() : "";
                    String providedToken = (parts.length >= 3) ? parts[2].trim() : null;

                    if (expectedToken != null && !expectedToken.equals(providedToken)) {
                        log("Reject HEARTBEAT " + role + " from " + sender + " - invalid token");
                        continue;
                    }

                    synchronized (regLock) {
                        if ("transmitter".equals(role) && transmitter.value != null
                                && transmitter.value.addr.equals(sender)) {
                            transmitter.value.touch();
                            //log("Heartbeat transmitter " + sender);
                        } else if ("receiver".equals(role) && receiver.value != null
                                && receiver.value.addr.equals(sender)) {
                            receiver.value.touch();
                            //log("Heartbeat receiver " + sender);
                        } else {
                            log("Received HEARTBEAT from unregistered or mismatched addr: " + sender);
                        }
                    }
                    continue;
                }

                // Purge expired registrations
                long now = System.currentTimeMillis();
                synchronized (regLock) {
                    if (transmitter.value != null && now - transmitter.value.lastSeen > REGISTRY_EXPIRE_MS) {
                        log("Transmitter expired: " + transmitter.value.addr);
                        transmitter.value = null;
                    }
                    if (receiver.value != null && now - receiver.value.lastSeen > REGISTRY_EXPIRE_MS) {
                        log("Receiver expired: " + receiver.value.addr);
                        receiver.value = null;
                    }
                }

                // If no pair -> ignore traffic
                ClientInfo tx, rx;
                synchronized (regLock) {
                    tx = transmitter.value;
                    rx = receiver.value;
                }
                if (tx == null || rx == null) {
                    // Not ready yet
                    continue;
                }

                // Validate source is either registered transmitter or receiver; otherwise ignore (prevents spoofing)
                InetSocketAddress senderAddr = sender;
                InetSocketAddress target = null;
                if (senderAddr.equals(rx.addr)) {
                    target = tx.addr;
                } else if (senderAddr.equals(tx.addr)) {
                    target = rx.addr;
                } else {
                    // Packet from an unknown source; ignore
                    log("Dropping packet from unknown source " + senderAddr);
                    continue;
                }

                // Forward raw packet bytes (respect offset/len)
                byte[] forwardData = new byte[len];
                System.arraycopy(p.getData(), off, forwardData, 0, len);
                DatagramPacket out = new DatagramPacket(forwardData, forwardData.length, target.getAddress(), target.getPort());
                socket.send(out);
                log("Relayed " + len + " bytes from " + senderAddr + " -> " + target);
            } catch (SocketTimeoutException ste) {
                // timeout used to loop and check running/expiry
            } catch (IOException ioe) {
                log("Relay I/O error: " + ioe.getMessage());
            } catch (Throwable t) {
                log("Relay unexpected error: " + t.getMessage());
            }
        }

        socket.close();
        log("Relay stopped.");
    }

    /* ----------------------------
       Transmitter (LAN side) implementation
       ---------------------------- */
    private static void runTransmitter(String relayHost, int relayPort, String multicastGroup, int multicastPort, String token) throws Exception {
        InetAddress relayAddr = InetAddress.getByName(relayHost);
        DatagramSocket toRelay = new DatagramSocket();
        toRelay.setReuseAddress(true);
        toRelay.setSoTimeout(2000);
        log("Transmitter starting. Relay=" + relayHost + ":" + relayPort + " multicast=" + multicastGroup + ":" + multicastPort + " token=" + (token != null));

        // Register initially and periodically (thread)
        Runnable registerTask = () -> {
            try {
                String reg = "REGISTER transmitter" + (token != null ? (" " + token) : "");
                byte[] regb = reg.getBytes("UTF-8");
                DatagramPacket regp = new DatagramPacket(regb, regb.length, relayAddr, relayPort);
                toRelay.send(regp);
            } catch (IOException e) {
                log("Transmitter register error: " + e.getMessage());
            }
        };
        registerTask.run();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tx-register");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(registerTask, 10, 10, TimeUnit.SECONDS);

        // Small thread pool to handle queries (limit resources)
        ExecutorService queryPool = Executors.newFixedThreadPool(QUERY_SOCKET_POOL, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        // Buffer for incoming packets from relay
        byte[] buf = new byte[BUFFER_SIZE];

        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                toRelay.receive(p); // this socket receives packets forwarded from relay
                int off = p.getOffset();
                int len = p.getLength();
                if (len < 2) {
                    continue;
                }

                // First two bytes are original port (big-endian)
                byte[] data = p.getData();
                int origPort = ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
                byte[] queryData = new byte[len - 2];
                System.arraycopy(data, off + 2, queryData, 0, len - 2);

                // Submit query handler (full pool)
                queryPool.submit(() -> {
                    try {
                        InetAddress group = InetAddress.getByName(multicastGroup);
                        // Enumerar todas las direcciones IPv4 no loopback
                        for (NetworkInterface nif : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
                            if (!nif.isUp() || nif.isLoopback() || !nif.supportsMulticast()) {
                                continue;
                            }
                            for (InetAddress addr : java.util.Collections.list(nif.getInetAddresses())) {
                                if (!(addr instanceof Inet4Address) || addr.isLoopbackAddress()) {
                                    continue;
                                }

                                // Crear socket multicast para cada IP
                                MulticastSocket ms = null;
                                try {
                                    ms = new MulticastSocket(new InetSocketAddress(addr, 0));
                                    ms.setNetworkInterface(nif);
                                    ms.setTimeToLive(32);
                                    ms.setReuseAddress(true);
                                    ms.setSoTimeout(TRANSMITTER_RESP_TIMEOUT_MS);

                                    DatagramPacket sendQuery = new DatagramPacket(queryData, queryData.length, group, multicastPort);
                                    ms.send(sendQuery);
                                    log("TX multicast query (origPort=" + origPort + ") via " + addr.getHostAddress() + " (" + nif.getName() + ")");

                                    // Escucha de respuestas
                                    byte[] respBuf = new byte[BUFFER_SIZE];
                                    while (true) {
                                        DatagramPacket respPacket = new DatagramPacket(respBuf, respBuf.length);
                                        try {
                                            ms.receive(respPacket);
                                        } catch (SocketTimeoutException ste) {
                                            break; // timeout por cada interfaz
                                        }
                                        int rlen = respPacket.getLength();
                                        byte[] portBytes = new byte[]{(byte) (origPort >> 8), (byte) (origPort & 0xFF)};
                                        byte[] sendData = new byte[2 + rlen];
                                        System.arraycopy(portBytes, 0, sendData, 0, 2);
                                        System.arraycopy(respPacket.getData(), respPacket.getOffset(), sendData, 2, rlen);

                                        DatagramPacket out = new DatagramPacket(sendData, sendData.length, relayAddr, relayPort);
                                        toRelay.send(out);
                                        log("TX response " + rlen + " bytes from " + respPacket.getAddress().getHostAddress()
                                                + " via " + addr.getHostAddress() + " (" + nif.getName() + ")");
                                    }

                                } catch (Throwable t) {
                                    log("TX handler error [" + addr.getHostAddress() + "]: " + t.getMessage());
                                } finally {
                                    if (ms != null && !ms.isClosed()) {
                                        try {
                                            ms.close();
                                        } catch (Exception ignored) {
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log("TX global handler error: " + t.getMessage());
                    }
                });

            } catch (SocketTimeoutException ste) {
                // loop and check running
            } catch (IOException ioe) {
                log("Transmitter I/O error: " + ioe.getMessage());
            }
        }

        // shutdown
        scheduler.shutdownNow();
        queryPool.shutdownNow();
        toRelay.close();
        log("Transmitter stopped.");
    }

    /* ----------------------------
       Receiver (remote side) implementation for Linux
       some issues detected on Linux with Windows implementation
       ---------------------------- */
    private static void runReceiverLinux(String relayHost, int relayPort, String multicastGroup, int multicastPort, String token) throws Exception {
        InetAddress relayAddr = InetAddress.getByName(relayHost);
        ExecutorService ex = Executors.newCachedThreadPool();

        // sockets
        DatagramSocket toRelay = new DatagramSocket();
        toRelay.setReuseAddress(true);
        toRelay.setSoTimeout(2000);

        DatagramSocket injectSocket = new DatagramSocket(); // usado para inyectar respuestas hacia cliente o loopback
        injectSocket.setReuseAddress(true);

        DatagramSocket sendMulticast = new DatagramSocket();
        sendMulticast.setReuseAddress(true);

        // mapeo puertoOrigen -> cliente (ip:port)
        final ConcurrentHashMap<Integer, InetSocketAddress> portToClient = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, Long> portLastSeen = new ConcurrentHashMap<>();
        final long CLIENT_MAP_EXPIRE_MS = 30_000L;

        // registry / heartbeat
        Runnable registerTask = () -> {
            try {
                String reg = "REGISTER receiver" + (token != null ? (" " + token) : "");
                byte[] regb = reg.getBytes(StandardCharsets.UTF_8);
                DatagramPacket regp = new DatagramPacket(regb, regb.length, relayAddr, relayPort);
                toRelay.send(regp);
                // log("Receiver(Linux) REGISTER sent");
            } catch (IOException e) {
                logErr("[REGISTER] " + e.getMessage());
            }
        };
        registerTask.run();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rx-register");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(registerTask, 5, 5, TimeUnit.SECONDS);

        ScheduledExecutorService mapCleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "clientmap-cleaner");
            t.setDaemon(true);
            return t;
        });
        mapCleaner.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (Integer port : portLastSeen.keySet()) {
                Long ts = portLastSeen.get(port);
                if (ts == null || now - ts > CLIENT_MAP_EXPIRE_MS) {
                    portLastSeen.remove(port);
                    portToClient.remove(port);
                    // log("Cleaned mapping for port=" + port);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);

        // listener + forwarder: guarda mapping srcPort->srcAddr antes de enviar al relay
        final BiConsumer<DatagramSocket, String> listenAndForward = (sock, tag) -> {
            byte[] buf = new byte[BUFFER_SIZE];
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            while (running) {
                try {
                    sock.receive(p);
                    InetAddress src = p.getAddress();
                    int srcPort = p.getPort();
                    int len = p.getLength();
                    if (len <= 0) {
                        p.setLength(buf.length);
                        continue;
                    }

                    // actualizar mapping (clave = puerto origen que SADP usa)
                    InetSocketAddress srcAddr = new InetSocketAddress(src, srcPort);
                    portToClient.put(srcPort, srcAddr);
                    portLastSeen.put(srcPort, System.currentTimeMillis());
                    log("Mapped srcPort=" + srcPort + " -> " + srcAddr.getAddress().getHostAddress() + ":" + srcAddr.getPort() + " [" + tag + "]");

                    // empaquetar y enviar al relay: [origPort(2)] + payload
                    byte[] forwardData = new byte[len + 2];
                    forwardData[0] = (byte) (srcPort >> 8);
                    forwardData[1] = (byte) (srcPort & 0xFF);
                    System.arraycopy(p.getData(), p.getOffset(), forwardData, 2, len);

                    DatagramPacket out = new DatagramPacket(forwardData, forwardData.length, relayAddr, relayPort);
                    toRelay.send(out);
                    log("Receiver(Linux) fwdQuery " + len + " bytes from " + src.getHostAddress() + ":" + srcPort + " [" + tag + "]");
                    p.setLength(buf.length);
                } catch (IOException e) {
                    logErr("[LISTENER][" + tag + "] " + e.getMessage());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        };

        // 1) IPv4 ANY
        ex.submit(() -> {
            try {
                DatagramSocket s = new DatagramSocket(null);
                s.setReuseAddress(true);
                s.setBroadcast(true);
                s.bind(new InetSocketAddress("0.0.0.0", multicastPort));
                listenAndForward.accept(s, "IPv4/0.0.0.0");
            } catch (IOException e) {
                logErr("[IPv4/0.0.0.0] " + e.getMessage());
            }
        });

        // 2) Loopback
        ex.submit(() -> {
            try {
                DatagramSocket s = new DatagramSocket(null);
                s.setReuseAddress(true);
                s.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), multicastPort));
                listenAndForward.accept(s, "LOOPBACK/127.0.0.1");
            } catch (BindException be) {
                logErr("[LOOPBACK] bind failed: " + be.getMessage());
            } catch (IOException e) {
                logErr("[LOOPBACK] " + e.getMessage());
            }
        });

        // 3) IPv6 ANY
        ex.submit(() -> {
            try {
                DatagramChannel ch = DatagramChannel.open();
                ch.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, true);
                ch.bind(new InetSocketAddress(InetAddress.getByName("::"), multicastPort));
                DatagramSocket s = ch.socket();
                listenAndForward.accept(s, "IPv6/::");
            } catch (IOException e) {
                logErr("[IPv6] " + e.getMessage());
            }
        });

        // 4) Join multicast IPv4 on each suitable interface
        ex.submit(() -> {
            try {
                InetAddress group = InetAddress.getByName(multicastGroup);
                Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
                while (ifs.hasMoreElements()) {
                    NetworkInterface nif = ifs.nextElement();
                    try {
                        if (!nif.isUp() || nif.isLoopback() || !nif.supportsMulticast()) {
                            continue;
                        }
                    } catch (SocketException se) {
                        continue;
                    }
                    try {
                        MulticastSocket ms = new MulticastSocket(null);
                        ms.setReuseAddress(true);
                        ms.bind(new InetSocketAddress("0.0.0.0", multicastPort));
                        ms.setNetworkInterface(nif);
                        ms.joinGroup(new InetSocketAddress(group, multicastPort), nif);
                        log("[MCAST4] joined " + multicastGroup + " on " + nif.getName());
                        listenAndForward.accept(ms, "MCAST4/" + nif.getName());
                    } catch (IOException ioe) {
                        logErr("[MCAST4][" + nif.getName() + "] " + ioe.getMessage());
                    }
                }
            } catch (IOException e) {
                logErr("[MCAST4] " + e.getMessage());
            }
        });

        // 5) IPv6 multicast optional
        ex.submit(() -> {
            try {
                InetAddress group6 = InetAddress.getByName("ff02::c");
                Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
                while (ifs.hasMoreElements()) {
                    NetworkInterface nif = ifs.nextElement();
                    try {
                        if (!nif.isUp() || nif.isLoopback()) {
                            continue;
                        }
                    } catch (SocketException se) {
                        continue;
                    }
                    try {
                        MulticastSocket ms6 = new MulticastSocket(null);
                        ms6.setReuseAddress(true);
                        ms6.setNetworkInterface(nif);
                        ms6.bind(new InetSocketAddress(InetAddress.getByName("::"), multicastPort));
                        ms6.joinGroup(new InetSocketAddress(group6, multicastPort), nif);
                        log("[MCAST6] joined ff02::c on " + nif.getName());
                        listenAndForward.accept(ms6, "MCAST6/" + nif.getName());
                    } catch (IOException ioe) {
                        // ignore per-iface failures
                    }
                }
            } catch (IOException e) {
                logErr("[MCAST6] " + e.getMessage());
            }
        });

        log("Receiver(Linux) iniciado. Relay=" + relayHost + ":" + relayPort + " multicast=" + multicastGroup + ":" + multicastPort);

        // receptor de respuestas desde relay; reinyecta hacia cliente conocido o fallback a loopback + opcional multicast
        ex.submit(() -> {
            byte[] rbuf = new byte[BUFFER_SIZE];
            DatagramPacket rp = new DatagramPacket(rbuf, rbuf.length);
            while (running) {
                try {
                    toRelay.receive(rp); // recibe: [destPort(2)] + payload
                    int off = rp.getOffset();
                    int len = rp.getLength();
                    if (len < 2) {
                        rp.setLength(rbuf.length);
                        continue;
                    }
                    byte[] data = rp.getData();
                    int destPort = ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
                    int payloadLen = len - 2;
                    if (payloadLen <= 0) {
                        rp.setLength(rbuf.length);
                        continue;
                    }
                    byte[] payload = new byte[payloadLen];
                    System.arraycopy(data, off + 2, payload, 0, payloadLen);

                    // intentar enviar directamente al cliente que originó la query
                    InetSocketAddress clientAddr = portToClient.get(destPort);
                    if (clientAddr != null) {
                        try {
                            DatagramPacket inject = new DatagramPacket(payload, payloadLen, clientAddr.getAddress(), clientAddr.getPort());
                            injectSocket.send(inject);
                            log("Receiver(Linux) injected -> " + clientAddr.getAddress().getHostAddress() + ":" + clientAddr.getPort() + " size=" + payloadLen);
                        } catch (IOException e) {
                            logErr("[INJECT/CLIENT] " + e.getMessage());
                        }
                    } else {
                        // fallback a loopback
                        try {
                            DatagramPacket inject = new DatagramPacket(payload, payloadLen, InetAddress.getByName("127.0.0.1"), destPort);
                            injectSocket.send(inject);
                            log("Receiver(Linux) injected fallback -> 127.0.0.1:" + destPort + " size=" + payloadLen);
                        } catch (IOException e) {
                            logErr("[INJECT/FALLBACK] " + e.getMessage());
                        }
                    }

                    // además enviar al grupo multicast para cubrir aplicaciones que escuchan en grupo
                    try {
                        InetAddress group = InetAddress.getByName(multicastGroup);
                        DatagramPacket mcast = new DatagramPacket(payload, payloadLen, group, multicastPort);
                        sendMulticast.send(mcast);
                        // log("Receiver(Linux) also sent multicast payload size=" + payloadLen);
                    } catch (IOException e) {
                        logErr("[INJECT/MCAST] " + e.getMessage());
                    }

                    rp.setLength(rbuf.length);
                } catch (SocketTimeoutException ste) {
                    // continue
                } catch (IOException ioe) {
                    logErr("[TO_RELAY_RECV] " + ioe.getMessage());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        });

        // mantener vivo
        Thread.currentThread().join();

        // cleanup (shutdown hooks should normally stop execution before here)
        scheduler.shutdownNow();
        mapCleaner.shutdownNow();
        ex.shutdownNow();
        toRelay.close();
        injectSocket.close();
        sendMulticast.close();
        log("Receiver(Linux) stopped.");
    }

    /* ----------------------------
       Receiver (remote side) implementation for Windows
       ---------------------------- */
    private static void runReceiverWin(String relayHost, int relayPort, String multicastGroup, int multicastPort, String token) throws Exception {
        InetAddress relayAddr = InetAddress.getByName(relayHost);

        // Multicast socket to optionally join group (if local SADP uses multicast queries)
        // We keep original behavior: join group and forward multicast queries to relay prefixing source port.
        InetAddress group = InetAddress.getByName(multicastGroup);

        // Determine local address to bind multicast listening (important on multi-nic hosts)
        InetAddress localAddr = determineLocalAddress(group, multicastPort);
        NetworkInterface nif = NetworkInterface.getByInetAddress(localAddr);

        MulticastSocket ms = new MulticastSocket(new InetSocketAddress(localAddr, multicastPort));
        //MulticastSocket ms = new MulticastSocket(multicastPort);
        ms.setNetworkInterface(nif);
        ms.setReuseAddress(true);
        //ms.setLoopbackMode(false);
        //ms.setOption(java.net.StandardSocketOptions.IP_MULTICAST_LOOP, false);
        ms.joinGroup(new InetSocketAddress(group, multicastPort), nif);
        ms.setSoTimeout(2000);

        DatagramSocket toRelay = new DatagramSocket();
        toRelay.setReuseAddress(true);

        // Register with relay
        String reg = "REGISTER(Win) receiver" + (token != null ? (" " + token) : "");
        DatagramPacket regPacket = new DatagramPacket(reg.getBytes("UTF-8"), reg.length(), relayAddr, relayPort);
        toRelay.send(regPacket);
        log("Receiver(Win) registered to relay " + relayHost + ":" + relayPort + " joined multicast " + multicastGroup + ":" + multicastPort + " on " + localAddr + "/" + nif.getName());

        // Thread: read multicast queries and forward to relay with srcPort prefix
        Thread readerThread = new Thread(() -> {
            byte[] qbuf = new byte[BUFFER_SIZE];
            while (running) {
                try {
                    DatagramPacket packet = new DatagramPacket(qbuf, qbuf.length);
                    ms.receive(packet);
                    int srcPort = packet.getPort();
                    int qlen = packet.getLength();
                    if (qlen <= 0) {
                        continue;
                    }

                    byte[] portBytes = new byte[]{(byte) (srcPort >> 8), (byte) (srcPort & 0xFF)};
                    byte[] sendData = new byte[2 + qlen];
                    System.arraycopy(portBytes, 0, sendData, 0, 2);
                    System.arraycopy(packet.getData(), packet.getOffset(), sendData, 2, qlen);
                    DatagramPacket out = new DatagramPacket(sendData, sendData.length, relayAddr, relayPort);
                    toRelay.send(out);
                    log("Receiver(Win) forwarded query to relay (srcPort=" + srcPort + ") size=" + qlen);
                } catch (SocketTimeoutException ste) {
                    // continue to loop
                } catch (IOException ioe) {
                    log("Receiver(Win) multicast read I/O: " + ioe.getMessage());
                } catch (Throwable t) {
                    log("Receiver(Win) multicast unexpected: " + t.getMessage());
                }
            }
        }, "receiver-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        // Thread: periodic REGISTER/HEARTBEAT
        ScheduledExecutorService hb = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        Runnable heartbeatTask = () -> {
            try {
                String h = "HEARTBEAT(Win) receiver" + (token != null ? (" " + token) : "");
                DatagramPacket p = new DatagramPacket(h.getBytes("UTF-8"), h.length(), relayAddr, relayPort);
                toRelay.send(p);
            } catch (IOException e) {
                log("Receiver(Win) heartbeat error: " + e.getMessage());
            }
        };
        hb.scheduleAtFixedRate(heartbeatTask, 5, 5, TimeUnit.SECONDS);

        // Main loop: receive responses from relay and inject them as unicast to local SADP client on 127.0.0.1:origPort
        DatagramSocket unicastSender = new DatagramSocket(); // for sending to local SADP client
        byte[] rbuf = new byte[BUFFER_SIZE];
        while (running) {
            try {
                DatagramPacket p = new DatagramPacket(rbuf, rbuf.length);
                toRelay.receive(p); // replies come with 2-byte destPort + payload
                int off = p.getOffset();
                int len = p.getLength();
                if (len < 2) {
                    continue;
                }
                byte[] data = p.getData();
                int destPort = ((data[off] & 0xFF) << 8) | (data[off + 1] & 0xFF);
                int payloadLen = len - 2;
                byte[] payload = new byte[payloadLen];
                System.arraycopy(data, off + 2, payload, 0, payloadLen);

                InetAddress local = InetAddress.getByName("127.0.0.1");
                DatagramPacket resp = new DatagramPacket(payload, payload.length, local, destPort);
                unicastSender.send(resp);
                log("Receiver(Win) injected response to local SADP port " + destPort + " size=" + payloadLen);
            } catch (SocketTimeoutException ste) {
                // loop and check running
            } catch (IOException ioe) {
                log("Receiver(Win) toRelay I/O: " + ioe.getMessage());
            } catch (Throwable t) {
                log("Receiver(Win) unexpected: " + t.getMessage());
            }
        }

        // cleanup
        try {
            ms.leaveGroup(new InetSocketAddress(group, multicastPort), nif);
        } catch (IOException ignored) {
        }
        try {
            ms.close();
        } catch (Exception ignored) {
        }
        hb.shutdownNow();
        toRelay.close();
        unicastSender.close();
        log("Receiver(Win) stopped.");
    }

    /* ----------------------------
       Helpers
       ---------------------------- */
    // Determine a local address that will be used to reach the multicast group (works on multi-nic hosts).
    private static InetAddress determineLocalAddress(InetAddress remote, int remotePort) throws SocketException {
        try (DatagramSocket tmp = new DatagramSocket()) {
            // No data is sent; connect only chooses interface/local address
            tmp.connect(remote, remotePort);
            InetAddress local = tmp.getLocalAddress();
            if (local.isAnyLocalAddress() || local.isLoopbackAddress()) {
                // try fallback: pick first non-loopback
                for (NetworkInterface nif : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
                    for (InetAddress addr : java.util.Collections.list(nif.getInetAddresses())) {
                        if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                            return addr;
                        }
                    }
                }
            }
            return local;
        } catch (SocketException se) {
            // fallback: choose first non-loopback address
            for (NetworkInterface nif : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
                for (InetAddress addr : java.util.Collections.list(nif.getInetAddresses())) {
                    if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                        return addr;
                    }
                }
            }
            throw se;
        }
    }


    /* ----------------------------
       Utility & Registry structures
       ---------------------------- */
    private static void log(String s) {
        System.out.println("[" + Instant.now() + "] " + s);
    }

    private static void logErr(String s) {
        System.err.println("[" + SDF.format(new Date()) + "] ERROR: " + s);
    }

    private static class ClientInfo {

        final InetSocketAddress addr;
        volatile long lastSeen;

        ClientInfo(InetSocketAddress addr) {
            this.addr = addr;
            this.lastSeen = System.currentTimeMillis();
        }

        void touch() {
            this.lastSeen = System.currentTimeMillis();
        }
    }

    // simple holder since we want volatile reference semantics
    private static class Holder<T> {

        volatile T value;

        Holder(T v) {
            this.value = v;
        }
    }
}
