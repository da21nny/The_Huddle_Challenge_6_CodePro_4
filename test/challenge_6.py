import pandas as pd
import matplotlib.pyplot as plt

path = "docs/server_logs.csv" # Ruta del archivo CSV
df = pd.read_csv(path) # Carga del archivo CSV

df['timestamp_event'] = pd.to_datetime(df['timestamp_event']) # Convierte la columna timestamp_event a tipo datetime

df['is_bad_event'] = df['severity'].isin(['ERROR', 'CRITICAL']) | (df['status_code'] >= 500) # Define lo que es un "bad event" según el criterio: severidad de ERROR/CRITICAL o status >= 500

print("Libreria y data cargada correctamente")


# -------------------------------------------- 1


total_logs = len(df) # Total de logs
most_common_severity = df['severity'].value_counts().head() # Severidad mas frecuente
service_most_logs = df['service_name'].value_counts().idxmax() # Servicio con mas logs
service_least_logs = df['service_name'].value_counts().idxmin() # Servicio con menos logs
most_common_message = df['message'].value_counts().head(3) # Mensaje mas repetido
most_common_bad_message = df[df['is_bad_event']]['message'].value_counts().head(5) # Top 5 mensajes malos

print(f"Total de logs: {total_logs}\n")
print(f"Severity mas frecuente: {most_common_severity}\n")
print(f"Servicio con mas Logs: {service_most_logs}\n")
print(f"Servicio con menos Logs: {service_least_logs}\n")
print(f"Mensaje mas repetido: {most_common_message}\n")
print(f"Top 5 Mensajes Malos: {most_common_bad_message}\n")


# --------------------------------------------- 2


time_grouper = pd.Grouper(key='timestamp_event', freq='5min') # Agrupa los datos en ventanas de 5 minutos

df_time_windows = (
    df.groupby(time_grouper).agg(
        total_events = ('timestamp_event', 'count'),
        bad_events = ('is_bad_event', 'sum')
    ).reset_index().rename(columns={'timestamp_event': 'window_start'})
) # Agrupa los datos por ventana de 5 minutos y cuenta el total de eventos y bad events

df_time_windows['bad_rate'] = (
    df_time_windows['bad_events'] / df_time_windows['total_events']
) # Calcula el bad rate

df_windows_filtered = df_time_windows.loc[
    df_time_windows['total_events'] >= 20
] # Filtra las ventanas que tengan al menos 20 eventos

df_windows_top5 = (df_windows_filtered.sort_values('bad_rate', ascending=False).head(5)) # Ordena las ventanas por bad rate y toma las top 5
print(f"Top 5:\n{df_windows_top5}")

sr_critical_window = df_windows_top5.iloc[0] # Toma la ventana con el mayor bad rate
print(f"\nCritical windows:\n{sr_critical_window}")


# --------------------------------------------- 3


dt_window_start = sr_critical_window['window_start'] # Obtiene la ventana de tiempo con el mayor bad rate
dt_window_end = dt_window_start + pd.Timedelta('5min') # Obtiene la ventana de tiempo con el mayor bad rate

mask_window_start = df['timestamp_event'] >= dt_window_start # Filtra los datos por ventana de tiempo
mask_window_end = df['timestamp_event'] < dt_window_end # Filtra los datos por ventana de tiempo

df_critical = df.loc[mask_window_start & mask_window_end].copy() # Filtra los datos por ventana de tiempo
df_critical_bad = df_critical.loc[df_critical['is_bad_event']].copy() # Filtra los datos por ventana de tiempo

df_bad_by_service = (
    df_critical_bad
    .groupby('service_name')
    .agg(n_bad_events = ('is_bad_event', 'count'))
    .reset_index()
    .sort_values('n_bad_events', ascending=False)
) # Agrupa los datos por ventana de tiempo y cuenta el total de eventos y bad events
print(f"\nTabla: bad events por service_name (ranking):\n{df_bad_by_service}")

df_top5_bad_messages = (
    df_critical_bad['message'].value_counts().head(5).reset_index()
) # Obtiene los top 5 mensajes malos
print(f"\nTop 5 mensajes en bad events:\n{df_top5_bad_messages}")

df_top5_endpoints = (
    df_critical_bad.groupby('endpoint').agg(
        n_bad_events = ('is_bad_event', 'count'),
        n_5xx = ('status_code', lambda x: (x >= 500).sum()),
        avg_latency_ms = ('latency_ms', 'mean')
    ).reset_index().sort_values('n_bad_events', ascending=False).head(5)
) # Obtiene los top 5 endpoints más comprometidos
print(f"\nTop 5 endpoints más comprometidos:\n{df_top5_endpoints}")



# --------------------------------------------- 4



mask_incidente = (
    (df['timestamp_event'] >= dt_window_start) &
    (df['timestamp_event'] < dt_window_end)
) # Crea una máscara para filtrar los datos por ventana de tiempo
df_incidente = df.loc[mask_incidente].copy() # Filtra los datos por ventana de tiempo

df_baseline = df.loc[~mask_incidente].copy() # Filtra los datos por ventana de tiempo

n_total_incidente = df_incidente.shape[0] # Obtiene el total de eventos

n_bad_incidente = df_incidente['is_bad_event'].sum() # Obtiene el total de bad events
n_5xx_incidente = (df_incidente['status_code'] >= 500).sum() # Obtiene el total de 5xx

pct_bad_incidente = n_bad_incidente / n_total_incidente # Calcula el bad rate
pct_5xx_incidente = n_5xx_incidente / n_total_incidente # Calcula el 5xx rate
avg_latency_incidente = df_incidente['latency_ms'].mean() # Calcula el avg latency

n_total_baseline = df_baseline.shape[0] # Obtiene el total de eventos

n_bad_baseline = df_baseline['is_bad_event'].sum() # Obtiene el total de bad events
n_5xx_baseline = (df_baseline['status_code'] >= 500).sum() # Obtiene el total de 5xx

pct_bad_baseline = n_bad_baseline / n_total_baseline # Calcula el bad rate
pct_5xx_baseline = n_5xx_baseline / n_total_baseline # Calcula el 5xx rate
avg_latency_baseline = df_baseline['latency_ms'].mean() # Calcula el avg latency

df_comparacion = pd.DataFrame({
    'periodo'       : ['incidente', 'baseline'],
    'total_events'  : [n_total_incidente, n_total_baseline],
    'bad_rate'      : [pct_bad_incidente, pct_bad_baseline],
    'avg_latency_ms': [avg_latency_incidente, avg_latency_baseline],
    'pct_5xx'       : [pct_5xx_incidente, pct_5xx_baseline]
}) # Crea un dataframe con las métricas de incidente y baseline

df_comparacion = df_comparacion.set_index('periodo') # Establece el periodo como índice
df_comparacion = df_comparacion.round(4) # Redondea los valores a 4 decimales
print(f"\nQué cambió (Incidente vs Baseline):\n{df_comparacion}")




# --------------------------------------------- 5



time_grouper = pd.Grouper(key='timestamp_event', freq='5min') # Agrupa los datos por ventana de tiempo
df_severity_bins = (df.groupby([time_grouper, 'severity'])
                    .agg(n_events = ('timestamp_event', 'count'))
                    .reset_index()
                    ) # Agrupa los datos por ventana de tiempo y cuenta el total de eventos y bad events
df_severity_pivot = df_severity_bins.pivot_table(
    index = 'timestamp_event',
    columns = 'severity',
    values = 'n_events',
    aggfunc = 'sum'
).fillna(0) # Rellena los valores nulos con 0

sr_x_time = df_windows_filtered['window_start'] # Obtiene la ventana de tiempo con el mayor bad rate
sr_y_badrate = df_windows_filtered['bad_rate'] # Obtiene el bad rate

fig, ax = plt.subplots(figsize=(14, 5)) # Crea una figura y un eje para el gráfico

for col_severidad in df_severity_pivot.columns: # Itera sobre las columnas de severidad
    ax.plot(
        df_severity_pivot.index,
        df_severity_pivot[col_severidad],
        label = col_severidad
    ) # Grafica los datos

ax.axvline(
    x = dt_window_start,
    color = 'red',
    linestyle = '--',
    label = 'momento critico'
) # Grafica la línea vertical para el momento crítico

ax.set_title('Conteo de eventos por severidad en ventanas de 5 minutos') # Establece el título del gráfico
ax.set_xlabel('Tiempo') # Establece la etiqueta del eje x
ax.set_ylabel('Cantidad de eventos') # Establece la etiqueta del eje y
ax.legend() # Muestra la leyenda

fig.autofmt_xdate() # Formatea las fechas del eje x
plt.tight_layout() # Ajusta el diseño del gráfico
plt.show() # Muestra el gráfico


# --------------------------------------------- 6



fig, ax = plt.subplots(figsize=(14, 5)) # Crea una figura y un eje para el gráfico

ax.plot(
    sr_x_time,
    sr_y_badrate,
    color = 'orange',
    label = 'bad_rate'
) # Grafica los datos

ax.fill_between(
    sr_x_time,
    sr_y_badrate,
    alpha = 0.3,
    color = 'orange'
) # Rellena el área debajo de la curva

ax.axvline(
    x = dt_window_start,
    color = 'red',
    linestyle = '--',
    label = 'momento critico'
) # Grafica la línea vertical para el momento crítico

ax.set_title('Bad rate por ventana de 5 minutos') # Establece el título del gráfico
ax.set_xlabel('Tiempo') # Establece la etiqueta del eje x
ax.set_ylabel('Bad rate (bad_events / total_events)') # Establece la etiqueta del eje y
ax.legend() # Muestra la leyenda

fig.autofmt_xdate() # Formatea las fechas del eje x
plt.tight_layout() # Ajusta el diseño del gráfico
plt.show() # Muestra el gráfico