"""Productor (src/streaming/publicacion.py y fuente.py): plan, eventos danados y ritmo, sin Kafka."""
import pytest

from src.streaming import contrato as c
from src.streaming.fuente import ordenar_para_envio
from src.streaming.publicacion import CASOS_INVALIDOS, NOMBRE_DUPLICADO, Estadisticas, construir_plan, enviar
from kafka_en_memoria import BrokerEnMemoria, ProductorEnMemoria


def hogares(n):
    return [{
        'directorio': str(1000 + i), 'orden': '1', 'codigo_departamento': f'{5 + i % 3:02d}',
        'codigo_region': '1', 'codigo_clase': '1', 'nombre_severidad': 'Inseguridad leve',
        'factor_expansion': 100.0, 'prob_ia_moderada_grave': 20.0, 'prob_ia_grave': 2.0,
    } for i in range(n)]


def test_orden_de_envio_reproducible_y_limitado():
    base = hogares(50)
    a = ordenar_para_envio(base, semilla=42, limite=10)
    b = ordenar_para_envio(base, semilla=42, limite=10)
    assert a == b and len(a) == 10
    assert a != base[:10]
    assert len(ordenar_para_envio(base, 42)) == 50


def test_plan_sin_invalidos_es_un_evento_por_hogar():
    plan = construir_plan(hogares(20))
    assert len(plan) == 20
    assert all(p.estado_esperado == c.VALIDO for p in plan)


def test_plan_con_invalidos_cubre_las_cuatro_capas():
    plan = construir_plan(hogares(100), con_invalidos=True)
    danados = [p for p in plan if p.nombre_dano]
    assert len(plan) == 100 + len(CASOS_INVALIDOS) + 1
    esperados = sorted(p.estado_esperado for p in danados)
    assert esperados == sorted([c.INVALIDO_ESQUEMA] * 3 + [c.INVALIDO_VALORES, c.INVALIDO_REFERENCIA, c.DUPLICADO])
    # el duplicado sale despues del hogar original
    original = plan.index(next(p for p in plan if p.hogar == hogares(100)[0] and not p.nombre_dano))
    duplicado = plan.index(next(p for p in danados if p.nombre_dano == NOMBRE_DUPLICADO))
    assert duplicado > original


@pytest.mark.parametrize('nombre, estado, danar', CASOS_INVALIDOS)
def test_cada_dano_produce_el_estado_esperado_en_el_contrato(nombre, estado, danar):
    evento = c.construir_evento(hogares(1)[0])
    clave, valor = danar(dict(evento))
    resultado = c.validar_evento(valor, clave)
    # Referencia y duplicado los detecta el consumidor contra la base: para el contrato son validos
    esperado_contrato = c.VALIDO if estado in (c.INVALIDO_REFERENCIA, c.DUPLICADO) else estado
    assert resultado.estado == esperado_contrato, (nombre, resultado.motivo)


def test_enviar_respeta_el_ritmo_y_la_clave():
    broker = BrokerEnMemoria('encuestas-hogares')
    reloj = {'t': 0.0}
    esperas = []

    def dormir(s):
        esperas.append(s)
        reloj['t'] += s

    plan = construir_plan(hogares(30))
    estadisticas = Estadisticas(total=len(plan))
    enviar(ProductorEnMemoria(broker), 'encuestas-hogares', plan, 10, estadisticas,
           reloj=lambda: reloj['t'], dormir=dormir)
    assert estadisticas.entregados == 30
    assert reloj['t'] == pytest.approx(2.9)  # 30 eventos a 10/s: el ultimo sale a los 2,9 s
    # misma clave (departamento), misma particion
    por_clave = {}
    for m in broker.mensajes():
        por_clave.setdefault(m.key(), set()).add(m.partition())
    assert all(len(p) == 1 for p in por_clave.values())


def test_enviar_sin_ritmo_no_espera():
    broker = BrokerEnMemoria('encuestas-hogares')
    plan = construir_plan(hogares(10))
    estadisticas = Estadisticas(total=len(plan))
    enviar(ProductorEnMemoria(broker), 'encuestas-hogares', plan, None, estadisticas,
           dormir=lambda s: pytest.fail('no debia esperar'))
    assert estadisticas.entregados == 10


def test_un_error_de_entrega_detiene_el_envio():
    broker = BrokerEnMemoria('encuestas-hogares')
    plan = construir_plan(hogares(10))
    estadisticas = Estadisticas(total=len(plan))
    with pytest.raises(RuntimeError, match='no pudo entregar'):
        enviar(ProductorEnMemoria(broker, fallar_en=4), 'encuestas-hogares', plan, None, estadisticas)
    assert estadisticas.entregados == 3


def test_consumidor_se_detiene_si_kafka_no_responde():
    """Con el broker caido, el consumidor no queda esperando para siempre (hallazgo con Kafka real en Docker)."""
    from kafka_en_memoria import ConsumidorEnMemoria
    from src.streaming import procesamiento
    reloj = {'t': 0.0}

    class ConsumidorSinBroker(ConsumidorEnMemoria):
        broker_caido = True

        def consume(self, num_messages=1, timeout=None):
            reloj['t'] += 1.0  # cada consume vacio tarda 1 s
            return []

    parada = procesamiento.Parada()
    consumidor = ConsumidorSinBroker(BrokerEnMemoria('encuestas-hogares'), 'g', parada)
    with pytest.raises(RuntimeError, match='Kafka no responde'):
        procesamiento.consumir(consumidor, None, 'encuestas-hogares', parada, reloj=lambda: reloj['t'])
    limite = procesamiento.LIMITE_SIN_BROKER_S + 2 * procesamiento.SONDEO_BROKER_S
    assert reloj['t'] <= limite


def test_consumidor_sigue_si_kafka_responde():
    from kafka_en_memoria import ConsumidorEnMemoria
    from src.streaming import procesamiento
    reloj = {'t': 0.0}

    class ConsumidorOcioso(ConsumidorEnMemoria):
        def consume(self, num_messages=1, timeout=None):
            reloj['t'] += 1.0
            if reloj['t'] > 120:
                self.parada.pedir()
            return []

    parada = procesamiento.Parada()
    consumidor = ConsumidorOcioso(BrokerEnMemoria('encuestas-hogares'), 'g', parada)
    procesamiento.consumir(consumidor, None, 'encuestas-hogares', parada, reloj=lambda: reloj['t'])
    assert reloj['t'] > 120
