"""Contrato del evento de encuesta (src/streaming/contrato.py): pruebas unitarias, sin Kafka ni base."""
import json
from datetime import datetime, timezone

import pytest

from src.streaming import contrato as c

HOGAR = {
    'directorio': '7123456', 'orden': '1', 'codigo_departamento': '27', 'codigo_region': '4',
    'codigo_clase': '1', 'nombre_severidad': 'Inseguridad moderada', 'factor_expansion': 215.3,
    'prob_ia_moderada_grave': 63.2, 'prob_ia_grave': 12.5,
}


def evento(**cambios):
    base = c.construir_evento(HOGAR, momento=datetime(2026, 11, 17, 20, 0, tzinfo=timezone.utc))
    base.update(cambios)
    return base


def validar(ev, clave='27'):
    return c.validar_evento(c.serializar_evento(ev), clave.encode() if clave else None)


def test_evento_construido_cumple_el_contrato():
    ev = evento()
    assert list(ev) == list(c.CAMPOS)
    resultado = validar(ev)
    assert resultado.es_valido, resultado.motivo
    assert resultado.evento['momento_evento'] == datetime(2026, 11, 17, 20, 0, tzinfo=timezone.utc)
    assert ev['momento_evento'] == '2026-11-17T20:00:00.000Z'
    assert c.clave_evento(ev) == b'27'


def test_hogar_sin_dato_es_valido_con_probabilidades_nulas():
    ev = evento(nombre_severidad='Sin dato', prob_ia_moderada_grave=None, prob_ia_grave=None)
    assert validar(ev).es_valido


def test_los_cinco_niveles_de_severidad_del_dw():
    assert c.SEVERIDADES == ('Seguridad alimentaria', 'Inseguridad leve', 'Inseguridad moderada',
                             'Inseguridad grave', 'Sin dato')


@pytest.mark.parametrize('contenido, fragmento', [
    (b'{"version_contrato": 1, "id_ev', 'JSON mal formado'),
    (b'[1, 2]', 'no es un objeto JSON'),
    (b'\xff\xfe', 'UTF-8'),
    (None, 'vacio'),
])
def test_mensajes_ilegibles_son_invalido_esquema(contenido, fragmento):
    resultado = c.validar_evento(contenido)
    assert resultado.estado == c.INVALIDO_ESQUEMA
    assert fragmento in resultado.motivo


@pytest.mark.parametrize('campo', c.CAMPOS)
def test_falta_cualquier_campo_es_invalido_esquema(campo):
    ev = evento()
    del ev[campo]
    resultado = validar(ev)
    assert resultado.estado == c.INVALIDO_ESQUEMA
    assert f"falta el campo '{campo}'" in resultado.motivo


@pytest.mark.parametrize('cambios', [
    {'factor_expansion': 'alto'},
    {'factor_expansion': True},
    {'prob_ia_grave': '12'},
    {'version_contrato': '1'},
    {'directorio': 7123456},
    {'nombre_severidad': ''},
    {'id_evento': 'no-es-uuid'},
    {'momento_evento': '2026-11-17'},
    {'momento_evento': '2026-11-17T20:00:00'},  # sin zona horaria
])
def test_tipos_incorrectos_son_invalido_esquema(cambios):
    assert validar(evento(**cambios)).estado == c.INVALIDO_ESQUEMA


@pytest.mark.parametrize('cambios, fragmento', [
    ({'version_contrato': 2}, 'no soportada'),
    ({'directorio': '71A3456'}, 'solo digitos'),
    ({'orden': '123'}, 'hasta 2'),
    ({'codigo_departamento': '7'}, '2 digitos'),
    ({'codigo_clase': '12'}, '1 caracter'),
    ({'nombre_severidad': 'Inseguridad extrema'}, 'no es un nivel valido'),
    ({'factor_expansion': 0}, 'mayor que 0'),
    ({'factor_expansion': -5.0}, 'mayor que 0'),
    ({'prob_ia_moderada_grave': 150.0}, 'fuera de 0-100'),
    ({'prob_ia_grave': -1.0}, 'fuera de 0-100'),
    ({'prob_ia_moderada_grave': None, 'prob_ia_grave': None}, 'regla C12'),
    ({'nombre_severidad': 'Sin dato'}, 'regla C12'),
    ({'prob_ia_grave': 70.0}, 'no puede superar'),
])
def test_reglas_de_valores_son_invalido_valores(cambios, fragmento):
    resultado = validar(evento(**cambios))
    assert resultado.estado == c.INVALIDO_VALORES
    assert fragmento in resultado.motivo


def test_la_clave_debe_coincidir_con_el_departamento():
    resultado = validar(evento(), clave='05')
    assert resultado.estado == c.INVALIDO_VALORES
    assert 'no coincide' in resultado.motivo


def test_reporta_todos_los_motivos_de_la_capa_juntos():
    ev = evento()
    del ev['orden']
    del ev['factor_expansion']
    resultado = validar(ev)
    assert len(resultado.errores) == 2


def test_campos_extra_se_ignoran():
    ev = evento()
    ev['campo_nuevo'] = 'x'
    resultado = validar(ev)
    assert resultado.es_valido
    assert 'campo_nuevo' not in resultado.evento


def test_valores_no_finitos_no_rompen_la_validacion():
    texto = json.dumps(evento()).replace('215.3', 'NaN').encode()
    assert c.validar_evento(texto, b'27').estado == c.INVALIDO_VALORES


def test_momento_sin_zona_horaria_no_se_puede_construir():
    with pytest.raises(ValueError):
        c.construir_evento(HOGAR, momento=datetime(2026, 11, 17))
