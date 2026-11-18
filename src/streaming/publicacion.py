"""
Logica del productor: plan de envio, eventos danados de demostracion y envio a ritmo.

El script streaming/productor.py arma los argumentos y la conexion; aqui
queda lo que se puede probar sin Kafka (el productor de Kafka se inyecta).

Ritmo: se mide en eventos por segundo. Cada evento tiene una hora de salida
programada (inicio + n / eventos_por_segundo) y se espera solo lo necesario
para cumplirla, asi el ritmo no se degrada por el tiempo que tarda cada envio.
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from src.streaming import contrato as c

log = logging.getLogger('productor')

# '99' NO sirve: es Vichada en DIVIPOLA. '00' no corresponde a ningun departamento.
DEPARTAMENTO_INEXISTENTE = '00'


# --- Eventos danados de --con-invalidos ------------------------------------
# Cada caso parte de un evento valido y lo dana de una forma. Devuelve
# (clave, valor) del mensaje. El consumidor debe marcarlos con el estado
# esperado, guardarlos en el staging y seguir sin caerse.
def _sin_factor(evento):
    del evento[c.FACTOR]
    return c.clave_evento(evento), c.serializar_evento(evento)


def _texto_en_factor(evento):
    return c.clave_evento(evento), c.serializar_evento({**evento, c.FACTOR: 'alto'})


def _json_cortado(evento):
    valor = c.serializar_evento(evento)
    return c.clave_evento(evento), valor[: len(valor) // 2]


def _probabilidad_fuera_de_rango(evento):
    # Un hogar "Sin dato" tiene probabilidad nula: se le pone severidad con dato
    # para que la unica falla sea el rango.
    danado = {**evento, c.PROB_MG: 150.0, c.PROB_G: 10.0}
    if danado[c.SEVERIDAD] == c.SEVERIDAD_SIN_DATO:
        danado[c.SEVERIDAD] = 'Inseguridad moderada'
    return c.clave_evento(danado), c.serializar_evento(danado)


def _departamento_inexistente(evento):
    danado = {**evento, c.DEPARTAMENTO: DEPARTAMENTO_INEXISTENTE}
    return c.clave_evento(danado), c.serializar_evento(danado)


CASOS_INVALIDOS: tuple[tuple[str, str, Callable[[dict], tuple[bytes, bytes]]], ...] = (
    ('falta factor_expansion', c.INVALIDO_ESQUEMA, _sin_factor),
    ('texto en factor_expansion', c.INVALIDO_ESQUEMA, _texto_en_factor),
    ('JSON cortado', c.INVALIDO_ESQUEMA, _json_cortado),
    ('probabilidad 150', c.INVALIDO_VALORES, _probabilidad_fuera_de_rango),
    (f'departamento {DEPARTAMENTO_INEXISTENTE} inexistente', c.INVALIDO_REFERENCIA, _departamento_inexistente),
)
NOMBRE_DUPLICADO = 'hogar ya enviado'


@dataclass(frozen=True)
class EventoPlaneado:
    """Un mensaje por enviar: el hogar de origen y, si es de demostracion, como danarlo."""

    hogar: dict
    nombre_dano: str | None = None
    estado_esperado: str = c.VALIDO
    danar: Callable[[dict], tuple[bytes, bytes]] | None = None

    def generar(self) -> tuple[bytes, bytes, dict]:
        """(clave, valor, evento). momento_evento se sella aqui: es el instante de la emision."""
        evento = c.construir_evento(self.hogar)
        if self.danar is None:
            return c.clave_evento(evento), c.serializar_evento(evento), evento
        clave, valor = self.danar(dict(evento))
        return clave, valor, evento


def construir_plan(hogares: list[dict], con_invalidos: bool = False) -> list[EventoPlaneado]:
    """Orden final de envio. Los eventos danados se reparten a intervalos fijos entre los buenos.

    El caso DUPLICADO reenvia (con otro id_evento) un hogar que ya salio antes
    en el plan: el consumidor debe rechazarlo porque el hogar ya se recibio.
    """
    plan = [EventoPlaneado(hogar) for hogar in hogares]
    if not con_invalidos or not hogares:
        return plan
    casos = [EventoPlaneado(dict(hogares[0]), nombre, estado, danar) for nombre, estado, danar in CASOS_INVALIDOS]
    casos.append(EventoPlaneado(dict(hogares[0]), NOMBRE_DUPLICADO, c.DUPLICADO, None))
    paso = len(plan) / (len(casos) + 1)
    # De atras hacia adelante para que insertar uno no mueva la posicion de los demas.
    # Posicion minima 1: el duplicado siempre sale despues del hogar original.
    for indice in reversed(range(len(casos))):
        plan.insert(max(1, round(paso * (indice + 1))), casos[indice])
    return plan


@dataclass
class Estadisticas:
    """Conteo del envio. Se actualiza mientras avanza para informar aunque se interrumpa."""

    total: int
    entregados: int = 0
    errores: list = field(default_factory=list)


def enviar(productor: Any, topic: str, plan: list[EventoPlaneado], eventos_por_segundo: float | None,
           estadisticas: Estadisticas, reloj=time.monotonic, dormir=time.sleep, cada=500) -> None:
    """Publica el plan en orden, al ritmo pedido (None = sin pausa).

    La entrega se confirma de forma asincrona (callback). Si Kafka reporta un
    error de entrega, el envio se detiene (RuntimeError) en vez de seguir a ciegas.
    """
    def al_entregar(error, mensaje):
        if error is not None:
            estadisticas.errores.append(error.str() if hasattr(error, 'str') else str(error))
        else:
            estadisticas.entregados += 1

    inicio = reloj()
    for numero, planeado in enumerate(plan, start=1):
        if eventos_por_segundo:
            espera = inicio + (numero - 1) / eventos_por_segundo - reloj()
            if espera > 0:
                dormir(espera)
        clave, valor, evento = planeado.generar()
        while True:
            try:
                productor.produce(topic, key=clave, value=valor, on_delivery=al_entregar)
                break
            except BufferError:  # cola local llena: se atienden entregas y se reintenta
                productor.poll(0.5)
        productor.poll(0)
        if planeado.nombre_dano:
            log.info('[%s/%s] evento DANADO A PROPOSITO (%s; se espera %s)', numero, estadisticas.total,
                     planeado.nombre_dano, planeado.estado_esperado)
        elif numero % cada == 0 or numero == estadisticas.total:
            log.info('[%s/%s] publicados; entregados %s', numero, estadisticas.total, estadisticas.entregados)
        if estadisticas.errores:
            raise RuntimeError(f'Kafka no pudo entregar un evento ({estadisticas.errores[0]}): se detiene el envio')
    pendientes = productor.flush(30)
    if pendientes or estadisticas.errores:
        raise RuntimeError(f'Quedaron {pendientes} eventos sin confirmar; errores: {estadisticas.errores[:3]}')
