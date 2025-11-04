<h1>SADPRelay</h1>

SADPRelay es una herramienta ligera escrita en Java que permite retransmitir tráfico del protocolo SADP (Search Active Device Protocol) de Hikvision a través de Internet, de forma que un cliente remoto pueda descubrir y administrar cámaras IP como si estuviera en la misma red local (LAN).

Este programa actúa como un bridge entre el segmento de red donde se encuentran las cámaras y un cliente remoto que utiliza la aplicación oficial SADP. No requiere configuración avanzada, NAT traversal complejo ni múltiples clientes. Está diseñado para uso controlado en entornos cerrados o VPNs.

🚀 Características

- Relay transparente de paquetes UDP SADP.
- Permite descubrimiento y gestión de cámaras IP Hikvision desde ubicaciones remotas.
- No requiere dependencias externas.
- Compatible con redes multicast (239.255.255.250:37020 por defecto).
- Latencia mínima gracias a forwarding directo sin procesamiento adicional.
- No modifica el contenido del paquete, solo adjunta y restaura puertos origen/destino para mantener compatibilidad con el protocolo SADP original.
- Soporte para Windows, Linux y Android nativo.

🧭 Modos de operación

SADPRelay se compone de tres modos:

relay: punto intermedio que recibe tráfico y lo retransmite entre transmisor y receptor.

receiver: corre en el equipo local. Escucha el multicast SADP vía relay del equipo remoto.

transmitter: corre en la máquina remota. Captura las cámaras en cada subred a la que esté conectado y envía al relay.

🧪 Ejemplos de uso

<b>Relay</b>

<code>java SADPRelay relay 0.0.0.0 12345 239.255.255.250 37020</code><br>
0.0.0.0 - Para indicar que utilize la primera interfaz NIC activa.

<b>Receiver (Equipo remoto)</b>

<code>java SADPRelay receiver 1.2.3.4 12345 239.255.255.250 37020</code><br>

<b>Transmitter (Equipo en LAN con cámaras)</b>

<code>java SADPRelay transmitter 1.2.3.4 12345 239.255.255.250 37020</code><br>
1.2.3.4 - Es la dirección del servidor relay. Puede usar IP pública, IP de subred o nombre por DNS.

Luego, abre el SADP Tool oficial en la máquina remota. Las cámaras deberían aparecer como si estuvieras físicamente en la red local.

⚙️ Requisitos

- Java 8+
- Acceso UDP permitido (puertos usados: relayPort y 37020 UDP)
- Multicast habilitado en la red LAN de las cámaras.
- Configuración NAT o redireccionamiento de puertos en el relay si se usa sobre Internet.

🔐 Consideraciones de seguridad

- Se implementa autenticación y cifrado vía token.
- Recomendado usar únicamente en entornos controlados o sobre túneles VPN/IPsec/WireGuard.
- El relay no filtra tráfico, retransmite todo lo que recibe.

📦 Compilación

Puede copiar el código en un proyecto nuevo con IDE Java de su preferencia o directamente con la JDK y el comando:<br>
<code>javac SADPRelay.java</code>

🧠 Notas técnicas

- El tráfico SADP utiliza multicast para descubrimiento.<br>
- SADPRelay agrega dos bytes al inicio del paquete para conservar el puerto de origen.<br>
- El relay actúa como un passthrough bidireccional, sin alterar la carga útil.<br>
- No se soportan múltiples clientes simultáneos; está diseñado para un transmisor y un receptor.<br>

:wrench: Áreas de mejora

Buffers y performance<br>
   - Usar un pool de buffers (ByteBuffer) reutilizables para evitar muchas asignaciones y desperdicio de memoria.
   - Las copias las realizo con (System.arraycopy), pero podría usar ByteBuffer.wrap() y slices para reducir overhead en el relay.

Excepciones<br>
  - Separar IOException y RuntimeException para tener logs más claros.
  - Evitar imprimir todo t.getMessage() sin stacktrace en casos críticos; para debugging intenso mejor usar t.printStackTrace() o logErr(t).

Multicast en Linux
  - El listener multicast es bloqueante (sock.receive(p)) y se ejecuta en múltiples hilos.
  - Usar Selector con DatagramChannel no bloqueante para reducir hilos.

Heartbeat y REGISTER
  - Los envío cada 5-10s fijo. (Añadir jitter aleatorio para evitar sincronización de picos si hay muchos clientes).
  - Podría unificar REGISTER y HEARTBEAT en un solo paquete con flag tipo role|timestamp|token para simplificar parsing.

Seguridad
  - Token es opcional y en texto plano. Pensando en usar HMAC-SHA256 con timestamp para evitar replay attacks si se va a usar en redes no confiables.
  - Validación de port en relay y transmitter (correcto), añadir también a InetAddress (evitar 0.0.0.0 desde fuera de LAN).

Shutdown más limpio
  - Actualmente running = false y shutdown hooks, pero algunos hilos bloqueantes en receive() tardan en salir.
  - Usar socket.close() dentro de shutdown hook para forzar salida inmediata.

Logs
  - log() y logErr() imprimen todo en stdout/stderr. Para despliegues largos mejor usar un logger con rotación de archivos y niveles (INFO/DEBUG/ERROR).

Compatibilidad Android (Termux)
  - Funciona bien porque Termux soporta DatagramSocket y MulticastSocket (de momento).
  - Consideraré los permisos de red en Android: INTERNET y CHANGE_NETWORK_STATE (no son necesarios de momento) salvo si se necesitara multicast en algunas interfaces.

📜 Licencia

Este proyecto se distribuye bajo licencia MIT. Consulta el archivo LICENSE para más detalles.
