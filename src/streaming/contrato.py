"""
Contrato del evento de encuesta de hogar (version 1).

Unica fuente de verdad de la forma del evento que viaja por Kafka: el
productor arma cada evento con construir_evento y el consumidor valida cada
mensaje con validar_evento antes de guardarlo en el modelo.

Topic: encuestas-hogares (3 particiones). Clave del mensaje: el codigo del
departamento (2 digitos), que mantiene en orden los eventos de un mismo
departamento dentro de su particion.

Campos del evento (todos obligatorios):
    version_contrato        entero, igual a 1
    id_evento               texto UUID (identificador del evento)
    momento_evento          texto ISO-8601 con zona horaria (marca de tiempo)
    directorio, orden       texto de digitos: clave natural del hogar
    codigo_departamento     texto de 2 digitos; debe coincidir con la clave
    codigo_region           texto de 1 caracter
    codigo_clase            texto de 1 caracter
    nombre_severidad        uno de los 5 niveles de dim_nivel_severidad_ia
    factor_expansion        numero finito mayor que 0
    prob_ia_moderada_grave  numero 0-100 o null
    prob_ia_grave           numero 0-100 o null

Capas de validacion y estado resultante:
    INVALIDO_ESQUEMA     no se puede interpretar: no es JSON, falta un campo o
                         un campo tiene un tipo que no corresponde.
    INVALIDO_VALORES     la forma es correcta pero algun valor viola una regla.
    INVALIDO_REFERENCIA  (lo asigna el consumidor) un miembro no existe en las
                         dimensiones del DW.
    DUPLICADO            (lo asigna el consumidor) el hogar ya se recibio.

Las reglas de valores reutilizan las del batch (docs/reglas_calidad.md):
C2 (codigo de departamento), C12 (probabilidad nula si y solo si la
severidad es "Sin dato") y los rangos de la ECV. Asi el streaming acepta
exactamente lo que el DW dio por bueno.

Aqui no hay Kafka ni base de datos: solo funciones puras, faciles de probar.
"""
import json
import math
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from src.ecv.dimensional_model import NIVELES_SEVERIDAD
from src.streaming.config import VERSION_CONTRATO

# --- Campos ---------------------------------------------------------------
VERSION = 'version_contrato'
ID_EVENTO = 'id_evento'
MOMENTO_EVENTO = 'momento_evento'
DIRECTORIO = 'directorio'
ORDEN = 'orden'
DEPARTAMENTO = 'codigo_departamento'
REGION = 'codigo_region'
CLASE = 'codigo_clase'
SEVERIDAD = 'nombre_severidad'
FACTOR = 'factor_expansion'
PROB_MG = 'prob_ia_moderada_grave'
PROB_G = 'prob_ia_grave'

# Orden de los campos en el JSON del evento
CAMPOS = (VERSION, ID_EVENTO, MOMENTO_EVENTO, DIRECTORIO, ORDEN, DEPARTAMENTO, REGION, CLASE,
          SEVERIDAD, FACTOR, PROB_MG, PROB_G)
CAMPOS_TEXTO = (ID_EVENTO, MOMENTO_EVENTO, DIRECTORIO, ORDEN, DEPARTAMENTO, REGION, CLASE, SEVERIDAD)
PROBABILIDADES = (PROB_MG, PROB_G)

# --- Dominios (los mismos del DW) -----------------------------------------
SEVERIDADES = tuple(nivel['nombre_severidad'] for nivel in NIVELES_SEVERIDAD)
SEVERIDAD_SIN_DATO = 'Sin dato'
SEVERIDAD_GRAVE = 'Inseguridad grave'
LARGO_DIRECTORIO = 10   # VARCHAR(10) en fact_seguridad_alimentaria_hogar
LARGO_ORDEN = 2         # VARCHAR(2)
PROBABILIDAD_MAXIMA = 100.0  # las probabilidades FIES del DANE vienen en escala 0-100

# Regla "prob_ia_grave <= prob_ia_moderada_grave": la inseguridad grave es un
# subconjunto de la moderada o grave. Pendiente de confirmar con los datos
# reales del DW (consulta de verificacion de la Fase 2); si los datos la
# incumplen, se desactiva aqui y se documenta.
VALIDAR_GRAVE_MENOR_O_IGUAL = True
TOLERANCIA = 1e-9

# --- Estados del staging (coinciden con el CHECK de stg_evento_encuesta) ---
RECIBIDO = 'RECIBIDO'
VALIDO = 'VALIDO'
INVALIDO_ESQUEMA = 'INVALIDO_ESQUEMA'
INVALIDO_VALORES = 'INVALIDO_VALORES'
INVALIDO_REFERENCIA = 'INVALIDO_REFERENCIA'
DUPLICADO = 'DUPLICADO'
ESTADOS = (RECIBIDO, VALIDO, INVALIDO_ESQUEMA, INVALIDO_VALORES, INVALIDO_REFERENCIA, DUPLICADO)

_DIGITOS = re.compile(r'^\d+$')


@dataclass(frozen=True)
class ResultadoValidacion:
    """Resultado de validar un mensaje contra el contrato (capas de esquema y valores)."""

    estado: str
    errores: tuple[str, ...] = ()
    # Evento ya interpretado (solo si es valido): numeros como float o None y
    # momento_evento como datetime con zona horaria.
    evento: dict[str, Any] | None = None

    @property
    def es_valido(self) -> bool:
        return self.estado == VALIDO

    @property
    def motivo(self) -> str | None:
        """Todos los motivos en un solo texto (columna motivo del staging)."""
        return '; '.join(self.errores) if self.errores else None


# ---------------------------------------------------------------------------
# Construccion (productor)
# ---------------------------------------------------------------------------
def formatear_momento(momento: datetime) -> str:
    """Instante -> texto ISO-8601 en UTC con milisegundos, p. ej. 2026-11-17T20:15:00.123Z."""
    if momento.tzinfo is None:
        raise ValueError('momento_evento necesita zona horaria (usar datetime.now(timezone.utc))')
    return momento.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def _numero_o_nulo(valor: Any) -> float | None:
    if valor is None:
        return None
    numero = float(valor)
    return None if math.isnan(numero) else numero


def construir_evento(hogar: Mapping[str, Any], *, id_evento: str | None = None,
                     momento: datetime | None = None) -> dict[str, Any]:
    """Convierte una fila del hecho de hogares (con su geografia y severidad) en un evento.

    Genera un id_evento unico (UUID al azar) y sella momento_evento con el
    instante de la emision.
    """
    return {
        VERSION: VERSION_CONTRATO,
        ID_EVENTO: id_evento or str(uuid.uuid4()),
        MOMENTO_EVENTO: formatear_momento(momento or datetime.now(timezone.utc)),
        DIRECTORIO: str(hogar[DIRECTORIO]),
        ORDEN: str(hogar[ORDEN]),
        DEPARTAMENTO: str(hogar[DEPARTAMENTO]),
        REGION: str(hogar[REGION]),
        CLASE: str(hogar[CLASE]),
        SEVERIDAD: str(hogar[SEVERIDAD]),
        FACTOR: float(hogar[FACTOR]),
        PROB_MG: _numero_o_nulo(hogar[PROB_MG]),
        PROB_G: _numero_o_nulo(hogar[PROB_G]),
    }


def serializar_evento(evento: Mapping[str, Any]) -> bytes:
    """Evento -> JSON en UTF-8 (el valor del mensaje de Kafka)."""
    return json.dumps(evento, ensure_ascii=False).encode('utf-8')


def clave_evento(evento: Mapping[str, Any]) -> bytes:
    """Clave del mensaje: el codigo del departamento."""
    return str(evento[DEPARTAMENTO]).encode('utf-8')


# ---------------------------------------------------------------------------
# Validacion (consumidor)
# ---------------------------------------------------------------------------
def validar_evento(contenido: bytes | str | Mapping[str, Any] | None,
                   clave: bytes | str | None = None) -> ResultadoValidacion:
    """Valida un mensaje (bytes, texto JSON o dict) contra el contrato.

    Nunca lanza excepcion por el contenido: un evento invalido es un
    resultado, no un error del programa. Reporta todos los motivos de la capa
    que fallo. Los campos que sobran se ignoran. Si se pasa la clave del
    mensaje, se exige que coincida con codigo_departamento.
    """
    datos, error = _decodificar(contenido)
    if error is not None:
        return ResultadoValidacion(INVALIDO_ESQUEMA, (error,))
    errores = _errores_esquema(datos)
    if errores:
        return ResultadoValidacion(INVALIDO_ESQUEMA, tuple(errores))
    errores = _errores_valores(datos, clave)
    if errores:
        return ResultadoValidacion(INVALIDO_VALORES, tuple(errores))
    return ResultadoValidacion(VALIDO, (), _interpretar(datos))


def leer_momento(valor: str) -> datetime:
    """Texto ISO-8601 con zona horaria -> datetime. Lanza ValueError si no lo es."""
    try:
        date.fromisoformat(valor)
    except ValueError:
        pass
    else:
        raise ValueError('solo trae la fecha, falta la hora')
    momento = datetime.fromisoformat(valor)
    if momento.tzinfo is None:
        raise ValueError('falta la zona horaria')
    return momento


def _decodificar(contenido: Any) -> tuple[dict[str, Any] | None, str | None]:
    if contenido is None:
        return None, 'el mensaje esta vacio'
    if isinstance(contenido, Mapping):
        return dict(contenido), None
    if isinstance(contenido, (bytes, bytearray)):
        try:
            contenido = bytes(contenido).decode('utf-8')
        except UnicodeDecodeError:
            return None, 'el mensaje no es texto UTF-8'
    if not isinstance(contenido, str):
        return None, f'el mensaje no es texto ni JSON (llego {type(contenido).__name__})'
    try:
        datos = json.loads(contenido)
    except (ValueError, RecursionError) as error:
        detalle = f'{error.msg} en la posicion {error.pos}' if isinstance(error, json.JSONDecodeError) else str(error)
        return None, f'JSON mal formado: {detalle}'
    if not isinstance(datos, dict):
        return None, f'el mensaje no es un objeto JSON (llego {_describir(datos)})'
    return datos, None


def _describir(valor: Any) -> str:
    """Nombre legible del tipo JSON de un valor, para los motivos."""
    if valor is None:
        return 'null'
    if isinstance(valor, bool):
        return 'booleano'
    if isinstance(valor, str):
        return f'texto {valor[:30]!r}'
    if isinstance(valor, (int, float)):
        return f'numero {str(valor)[:30]}'
    if isinstance(valor, list):
        return 'lista'
    if isinstance(valor, dict):
        return 'objeto'
    return type(valor).__name__


def _es_numero(valor: Any) -> bool:
    # bool es subclase de int en Python: true/false no cuentan como numero
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)


def _es_finito(valor: int | float) -> bool:
    try:
        return math.isfinite(float(valor))
    except OverflowError:
        return False


def _errores_esquema(datos: dict[str, Any]) -> list[str]:
    """Campos faltantes y tipos incorrectos."""
    errores = [f"falta el campo '{campo}'" for campo in CAMPOS if campo not in datos]
    if VERSION in datos and not (isinstance(datos[VERSION], int) and not isinstance(datos[VERSION], bool)):
        errores.append(f'{VERSION} debe ser un entero (llego {_describir(datos[VERSION])})')
    for campo in CAMPOS_TEXTO:
        if campo in datos and not (isinstance(datos[campo], str) and datos[campo].strip()):
            errores.append(f'{campo} debe ser un texto no vacio (llego {_describir(datos[campo])})')
    if FACTOR in datos and not _es_numero(datos[FACTOR]):
        errores.append(f'{FACTOR} debe ser un numero (llego {_describir(datos[FACTOR])})')
    for campo in PROBABILIDADES:
        if campo in datos and datos[campo] is not None and not _es_numero(datos[campo]):
            errores.append(f'{campo} debe ser un numero o null (llego {_describir(datos[campo])})')
    if ID_EVENTO in datos and isinstance(datos[ID_EVENTO], str):
        try:
            uuid.UUID(datos[ID_EVENTO])
        except ValueError:
            errores.append(f'{ID_EVENTO} debe ser un UUID (llego {_describir(datos[ID_EVENTO])})')
    if MOMENTO_EVENTO in datos and isinstance(datos[MOMENTO_EVENTO], str):
        try:
            leer_momento(datos[MOMENTO_EVENTO])
        except ValueError:
            errores.append(f'{MOMENTO_EVENTO} debe ser fecha y hora ISO-8601 con zona horaria '
                           f'(llego {_describir(datos[MOMENTO_EVENTO])})')
    return errores


def _texto_clave(clave: bytes | str | None) -> str | None:
    if clave is None:
        return None
    if isinstance(clave, (bytes, bytearray)):
        return bytes(clave).decode('utf-8', errors='replace')
    return str(clave)


def _errores_valores(datos: dict[str, Any], clave: bytes | str | None) -> list[str]:
    """Reglas de valores. Supone que el esquema ya se valido."""
    errores = []
    if datos[VERSION] != VERSION_CONTRATO:
        errores.append(f'{VERSION} {datos[VERSION]} no soportada (se espera {VERSION_CONTRATO})')
    for campo, largo in ((DIRECTORIO, LARGO_DIRECTORIO), (ORDEN, LARGO_ORDEN)):
        valor = datos[campo]
        if not _DIGITOS.match(valor) or len(valor) > largo:
            errores.append(f'{campo} debe tener solo digitos y hasta {largo} caracteres (llego {valor[:20]!r})')
    departamento = datos[DEPARTAMENTO]
    if not re.match(r'^\d{2}$', departamento):
        errores.append(f'{DEPARTAMENTO} debe tener 2 digitos (llego {departamento[:10]!r})')   # C2
    texto_clave = _texto_clave(clave)
    if texto_clave is not None and texto_clave != departamento:
        errores.append(f'la clave del mensaje ({texto_clave[:10]!r}) no coincide con '
                       f'{DEPARTAMENTO} ({departamento[:10]!r})')
    for campo in (REGION, CLASE):
        if len(datos[campo]) != 1:
            errores.append(f'{campo} debe tener 1 caracter (llego {datos[campo][:10]!r})')
    severidad = datos[SEVERIDAD]
    if severidad not in SEVERIDADES:
        errores.append(f'{SEVERIDAD} {severidad[:30]!r} no es un nivel valido')
    factor = datos[FACTOR]
    if not _es_finito(factor) or factor <= 0:
        errores.append(f'{FACTOR} debe ser un numero finito mayor que 0 (llego {factor})')
    for campo in PROBABILIDADES:
        valor = datos[campo]
        if valor is not None and (not _es_finito(valor) or not 0 <= valor <= PROBABILIDAD_MAXIMA):
            errores.append(f'{campo} {valor} fuera de 0-{PROBABILIDAD_MAXIMA:g}')
    # C12: probabilidad nula si y solo si la severidad es "Sin dato"
    sin_dato = severidad == SEVERIDAD_SIN_DATO
    for campo in PROBABILIDADES:
        if sin_dato and datos[campo] is not None:
            errores.append(f'{campo} debe ser null cuando la severidad es "Sin dato" (regla C12)')
        if not sin_dato and severidad in SEVERIDADES and datos[campo] is None:
            errores.append(f'{campo} es null pero la severidad es {severidad!r} (regla C12)')
    mg, g = datos[PROB_MG], datos[PROB_G]
    if (VALIDAR_GRAVE_MENOR_O_IGUAL and _es_numero(mg) and _es_numero(g) and _es_finito(mg) and _es_finito(g)
            and g > mg + TOLERANCIA):
        errores.append(f'{PROB_G} ({g}) no puede superar a {PROB_MG} ({mg})')
    return errores


def _interpretar(datos: dict[str, Any]) -> dict[str, Any]:
    """Evento valido con tipos de Python (solo los campos del contrato)."""
    evento = {campo: datos[campo] for campo in CAMPOS}
    evento[FACTOR] = float(datos[FACTOR])
    for campo in PROBABILIDADES:
        evento[campo] = None if datos[campo] is None else float(datos[campo])
    evento[MOMENTO_EVENTO] = leer_momento(datos[MOMENTO_EVENTO])
    return evento
