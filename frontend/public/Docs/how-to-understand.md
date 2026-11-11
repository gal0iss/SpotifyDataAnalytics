# How to understand Listening Data

## Guía para entender los gráficos y porcentajes

Los gráficos y métricas del detalle de artista usan los filtros seleccionados
(año y dispositivo). "General" significa toda la escucha de la cuenta dentro de
esos mismos filtros; no es una comparación con otros usuarios de Spotify.

## Métricas principales

- **Reproducciones:** cantidad de eventos de escucha registrados para el artista.
- **Tiempo de escucha:** suma del tiempo escuchado registrado en cada evento,
  convertido a horas. No representa necesariamente la duración completa de las
  canciones.
- **Tracks y álbumes únicos:** cantidad de tracks y álbumes distintos asociados
  con el artista.
- **Duración promedio por reproducción:** tiempo escuchado promedio por evento.
  La comparación "General" usa el promedio de todos los eventos filtrados.
- **Tasa de saltos:** porcentaje de eventos marcados como saltados. "General"
  usa todos los eventos filtrados.
- **Uso de aleatorio** y **Reproducciones sin conexión:** porcentaje de eventos
  del artista registrados con esas condiciones.

## Participación del artista en el tiempo total

La tarjeta **Participación del tiempo total de escucha** responde: "¿Qué parte
del tiempo de escucha de todo el período corresponde a este artista?"

```text
horas escuchadas del artista / horas escuchadas de todos los artistas × 100
```

Por ejemplo, **6 %** significa que, de cada 100 horas escuchadas en el período y
dispositivo seleccionados, aproximadamente 6 fueron de ese artista. La
referencia **General: 100 %** es el total de escucha usado como denominador.

## Evolución mensual

- **Tiempo de escucha por mes:** horas registradas para el artista en cada mes.
- **Reproducciones por mes:** cantidad de eventos del artista en cada mes.
- **Mes con más escucha** y **Mes con menos escucha:** meses con el mayor y el
  menor tiempo de escucha registrado para el artista dentro de los datos
  filtrados.

Estos gráficos muestran valores absolutos, no porcentajes. Más reproducciones
no siempre implican más horas: pueden variar la duración escuchada en cada
evento.

## Catálogo: participación de reproducciones por track

El porcentaje de la tabla de tracks usa como denominador **todas las
reproducciones del artista**, no las reproducciones de todos los artistas:

```text
reproducciones del track / reproducciones del artista × 100
```

Por ejemplo, si un track de Radiohead tiene 374 reproducciones y muestra 7,9 %,
significa que representa aproximadamente el 7,9 % de las reproducciones de
Radiohead en los filtros activos. El total del artista es de alrededor de 4.700
reproducciones; el número es aproximado porque el porcentaje se redondea a un
decimal.

La tabla enseña solo los diez tracks más reproducidos. Por eso sus porcentajes
no tienen que sumar 100 %: las canciones que no aparecen también forman parte
del total. Las horas de la tabla son el tiempo acumulado para cada track.

## Patrones de escucha: artista frente a General

En los gráficos de **Reproducciones por día** y **Reproducciones por hora**, cada
barra muestra qué porcentaje de las reproducciones corresponde a esa categoría:

```text
porcentaje del artista = reproducciones del artista en la categoría
                        / reproducciones totales del artista × 100

porcentaje General = reproducciones de todos los artistas en la categoría
                     / reproducciones totales de todos los artistas × 100
```

Por ejemplo, si el sábado representa el 30 % para un artista y el 15 % en
General, ese artista concentra proporcionalmente más reproducciones los sábados
que la escucha global de la cuenta. Cada serie se normaliza por separado; se
comparan las dos barras de una misma categoría, no se suman entre sí.

En **Tiempo de escucha por dispositivo**, la comparación usa la proporción de
horas —no la proporción de reproducciones— que corresponde a cada dispositivo,
tanto para el artista como para General. Si se selecciona un dispositivo
específico en los filtros, el gráfico solo tiene en cuenta ese dispositivo.

## Países

**Reproducciones por país** muestra cantidades de eventos asociados a cada país
disponible. Presenta hasta los diez países con más eventos; no es un porcentaje
ni una ubicación precisa.

## Cómo leer las comparaciones

- **Participación del tiempo total:** proporción del tiempo global atribuible al
  artista; por definición, la referencia General es 100 %.
- **Duración promedio** y **Tasa de saltos:** compara el artista con todos los
  eventos que cumplen los mismos filtros.
- **Uso de aleatorio** y **Reproducciones sin conexión:** se muestran para el
  artista, sin una cifra General en esta vista.

Todos los porcentajes pueden presentar pequeñas diferencias al sumarlos por
efecto del redondeo.
