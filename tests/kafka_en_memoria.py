"""Sustituto en memoria de Kafka para las pruebas del streaming (sin broker).

Reproduce lo que el productor y el consumidor dan por hecho de Kafka:
- un topic con N particiones; la clave decide la particion (misma clave,
  misma particion) y cada particion asigna offsets consecutivos;
- un consumer group guarda el offset confirmado por particion; un consumidor
  nuevo del mismo grupo retoma desde ahi (y desde el inicio si no hay
  confirmados), asi que lo no confirmado se reentrega.

No reproduce la red, los rebalanceos ni la replicacion: eso se prueba con el
Kafka real del docker-compose (README, seccion 11).
"""
import zlib


class Mensaje:
    def __init__(self, topic, particion, desplazamiento, clave, valor):
        self._datos = (topic, particion, desplazamiento, clave, valor)

    def topic(self):
        return self._datos[0]

    def partition(self):
        return self._datos[1]

    def offset(self):
        return self._datos[2]

    def key(self):
        return self._datos[3]

    def value(self):
        return self._datos[4]

    def error(self):
        return None


class Particion:
    def __init__(self, numero):
        self.partition = numero


class BrokerEnMemoria:
    def __init__(self, topic, particiones=3):
        self.topic = topic
        self.particiones = [[] for _ in range(particiones)]
        self.confirmados = {}  # (grupo, particion) -> siguiente offset a leer

    def particion_de(self, clave):
        return zlib.crc32(clave or b'') % len(self.particiones)

    def mensajes(self):
        return [m for particion in self.particiones for m in particion]


class ProductorEnMemoria:
    """Interfaz minima de confluent_kafka.Producer usada por publicacion.enviar."""

    def __init__(self, broker, fallar_en=None):
        self.broker = broker
        self.pendientes = []
        self.fallar_en = fallar_en  # numero de mensaje cuya entrega falla (para probar el error)
        self.producidos = 0

    def produce(self, topic, key=None, value=None, on_delivery=None):
        assert topic == self.broker.topic
        self.producidos += 1
        self.pendientes.append((key, value, on_delivery, self.producidos == self.fallar_en))

    def poll(self, _espera=0):
        for clave, valor, callback, falla in self.pendientes:
            if falla:
                callback('broker no disponible', None)
                continue
            numero = self.broker.particion_de(clave)
            particion = self.broker.particiones[numero]
            mensaje = Mensaje(self.broker.topic, numero, len(particion), clave, valor)
            particion.append(mensaje)
            callback(None, mensaje)
        self.pendientes = []
        return 0

    def flush(self, _espera=None):
        self.poll()
        return 0

    def list_topics(self, topic=None, timeout=None):
        """Metadatos minimos para comprobar_topic del productor."""
        class _Topic:
            error = None
            partitions = {n: None for n in range(len(self.broker.particiones))}

        class _Metadatos:
            topics = {self.broker.topic: _Topic()}
        return _Metadatos()


class ConsumidorEnMemoria:
    """Interfaz minima de confluent_kafka.Consumer usada por procesamiento.consumir.

    detener_tras: numero de mensajes a entregar antes de pedir la parada (simula Ctrl+C).
    """

    def __init__(self, broker, grupo, parada=None, detener_tras=None):
        self.broker = broker
        self.grupo = grupo
        self.parada = parada
        self.detener_tras = detener_tras
        self.entregados = 0
        self.posicion = {n: broker.confirmados.get((grupo, n), 0) for n in range(len(broker.particiones))}

    def subscribe(self, topics, on_assign=None):
        assert topics == [self.broker.topic]
        if on_assign:
            on_assign(self, [Particion(n) for n in self.posicion])

    def poll(self, _espera=None):
        if self.detener_tras is not None and self.entregados >= self.detener_tras:
            self.parada.pedir()
            return None
        for numero, particion in enumerate(self.broker.particiones):
            if self.posicion[numero] < len(particion):
                mensaje = particion[self.posicion[numero]]
                self.posicion[numero] += 1
                self.entregados += 1
                return mensaje
        return None

    def commit(self, message=None, asynchronous=False):
        self.broker.confirmados[(self.grupo, message.partition())] = message.offset() + 1

    def list_topics(self, topic=None, timeout=None):
        """Sondeo del broker. Con broker_caido=True simula un Kafka que no responde."""
        if getattr(self, 'broker_caido', False):
            from confluent_kafka import KafkaException
            raise KafkaException('broker caido (simulado)')

        class _Topic:
            error = None

        class _Metadatos:
            topics = {self.broker.topic: _Topic()}
        return _Metadatos()

    def close(self):
        pass
