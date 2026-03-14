# Análisis de Logs Distribuido
import pandas as pd
import matplotlib.pyplot as plt

# 1. Carga de datos
# Asegúrate de colocar el archivo CSV en el mismo directorio o cambiar la ruta
file_path = '../docs/server_logs.csv' 
try:
    df = pd.read_csv(file_path)
    
    # 2. Preprocesamiento de datos
    # Convertimos la columna timestamp_event a tipo datetime para poder operar por tiempos
    df['timestamp_event'] = pd.to_datetime(df['timestamp_event'])
    
    # Definimos lo que es un "bad event" según el criterio: severidad de ERROR/CRITICAL o status >= 500
    df['is_bad_event'] = df['severity'].isin(['ERROR', 'CRITICAL']) | (df['status_code'] >= 500)

    # 6.1 Exploración inicial
    total_logs = len(df)
    most_common_severity = df['severity'].mode()[0]
    service_most_logs = df['service_name'].value_counts().idxmax()
    service_least_logs = df['service_name'].value_counts().idxmin()
    most_common_message = df['message'].mode()[0]
    
    # Extraemos el df que solo contiene bad events para sacar el mensaje más repetido
    most_common_bad_message = df[df['is_bad_event']]['message'].mode()[0]

    # Mostramos los resultados en crudo
    print("=== 6.1 Exploración Inicial ===")
    print(f"Total de logs: {total_logs}")
    print(f"Severidad más común: {most_common_severity}")
    print(f"Servicio con más logs: {service_most_logs}")
    print(f"Servicio con menos logs: {service_least_logs}")
    print(f"Mensaje más repetido: {most_common_message}")
    print(f"Mensaje 'malo' más repetido: {most_common_bad_message}\n")

    # 6.2 Detección del momento crítico
    print("=== 6.2 Detección del Momento Crítico ===")
    # Agrupamos los datos estableciendo el timestamp como índice temporal en ventanas de 5 minutos
    df_grouped_5min = df.set_index('timestamp_event').resample('5min')
    
    # Generamos nuestra tabla con métricas por ventana
    windows_df = pd.DataFrame({
        'total_events': df_grouped_5min.size(),
        'bad_events': df_grouped_5min['is_bad_event'].sum()
    })
    
    # Reiniciamos el índice para tener la ventana como una columna normal
    windows_df = windows_df.reset_index().rename(columns={'timestamp_event': 'window_start'})
    
    # Calculamos bad_rate (Tasa de eventos malos)
    windows_df['bad_rate'] = windows_df['bad_events'] / windows_df['total_events']
    
    # Filtro: Solo nos importan las ventanas con 20 o más eventos
    windows_filtered = windows_df[windows_df['total_events'] >= 20]
    
    # Ordenamos de mayor a menor y extraemos el top 5
    top_5_critical = windows_filtered.sort_values(by='bad_rate', ascending=False).head(5)
    print("Top 5 momentos críticos:")
    print(top_5_critical.to_string(index=False))
    
    # El Momento Crítico absoluto es simplemente el primer registro del top 5
    critical_moment_start = top_5_critical.iloc[0]['window_start']
    # Como los bins son de 5 minutos, definimos el fin para futuros filtros
    critical_moment_end = critical_moment_start + pd.Timedelta(minutes=5)
    
    print(f"\n-> Momento crítico seleccionado: de {critical_moment_start} a {critical_moment_end}\n")

    # 6.3 Diagnóstico dentro del momento crítico
    print("=== 6.3 Diagnóstico dentro del Momento Crítico ===")
    
    # Filtramos nuestro dataset general para solo ver el fragmento de tiempo del incidente
    incident_df = df[(df['timestamp_event'] >= critical_moment_start) & 
                     (df['timestamp_event'] < critical_moment_end)]
    
    # Y de ahí, extraemos solo los datos defectuosos
    incident_bad_events = incident_df[incident_df['is_bad_event']]
    
    # Ranking de bad events por servicio
    bad_events_by_service = incident_bad_events['service_name'].value_counts()
    print("Bad events por servicio (Ranking):")
    print(bad_events_by_service.to_string())
    
    # Top 5 de los mensajes malos durante el incidente
    top_5_messages = incident_bad_events['message'].value_counts().head(5)
    print("\nTop 5 mensajes de 'bad events':")
    print(top_5_messages.to_string())
    
    # Criterio elegido explícitamente: Por cantidad de bad events generados en cada endpoint
    top_5_endpoints_by_bad_events = incident_bad_events['endpoint'].value_counts().head(5)
    print("\nTop 5 endpoints más comprometidos (Criterio: Por cantidad absoluta de bad events):")
    print(top_5_endpoints_by_bad_events.to_string(), "\n")

    # 6.4 “Qué cambió” (Incidente vs Baseline)
    print("=== 6.4 Comparación Incidente vs Baseline ===")
    
    # El Baseline representa la normalidad: es cualquier evento que NO esté en la ventana crítica
    baseline_df = df[(df['timestamp_event'] < critical_moment_start) | 
                     (df['timestamp_event'] >= critical_moment_end)]
                     
    # Función auxiliar para generar las métricas de un dataframe dado de forma limpia
    def calculate_metrics(_df):
        total = len(_df)
        if total == 0:
            return [0, 0, 0, 0]
        
        bad_rate = _df['is_bad_event'].sum() / total
        avg_latency = _df['latency_ms'].mean()
        perc_5xx = (_df['status_code'] >= 500).sum() / total * 100
        
        return [total, round(bad_rate, 4), round(avg_latency, 2), round(perc_5xx, 2)]
        
    incident_metrics = calculate_metrics(incident_df)
    baseline_metrics = calculate_metrics(baseline_df)
    
    # Armamos la tabla comparativa final
    comparison_df = pd.DataFrame(
        [incident_metrics, baseline_metrics], 
        index=['Momento Crítico', 'Baseline'],
        columns=['total_events', 'bad_rate', 'avg_latency_ms', '%_5xx']
    )
    print(comparison_df.to_string())

    # 7. Gráficos Obligatorios
    print("\nGenerando gráficos...")
    
    # --- GRÁFICO 1: Eventos por severidad en bins de 5 min ---
    # Usamos pd.Grouper para agrupar en columnas por severidad y filas por tiempo de 5 minutos
    severity_counts = df.groupby([pd.Grouper(key='timestamp_event', freq='5min'), 'severity']).size().unstack(fill_value=0)
    
    # Nos aseguramos de tener la base de las 4 severidades posibles requeridas
    for col in ['INFO', 'WARN', 'ERROR', 'CRITICAL']:
        if col not in severity_counts.columns:
            severity_counts[col] = 0
            
    # Graficamos el Gráfico 1
    plt.figure(figsize=(10, 5))
    severity_counts[['INFO', 'WARN', 'ERROR', 'CRITICAL']].plot(kind='line', ax=plt.gca())
    plt.title('Gráfico #1: Conteo de Eventos por Severidad en ventanas de 5 min')
    plt.xlabel('Hora / Tiempo')
    plt.ylabel('Cantidad Registrada')
    plt.legend(title='Severidad')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.show()
    
    # --- GRÁFICO 2: bad_rate en bins de 5 min ---
    # Usamos directamente el df temporal de ventanas que creamos en el punto 6.2
    plt.figure(figsize=(10, 5))
    windows_df.set_index('window_start')['bad_rate'].plot(kind='line', color='darkred', linewidth=2)
    plt.title('Gráfico #2: Evolución de Tasa de Errores (Bad Rate) por 5 min')
    plt.xlabel('Hora / Tiempo')
    plt.ylabel('Bad Rate (Ratio entre 0 y 1)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.show()

    # 8. Conclusiones Finales (Entrega)
    
    # Obtenemos los insights directamente de los cálculos previos
    top_service = bad_events_by_service.index[0]
    top_endpoint = top_5_endpoints_by_bad_events.index[0]
    top_message = top_5_messages.index[0]
    inc_bad_rate = incident_metrics[1] * 100
    base_bad_rate = baseline_metrics[1] * 100
    
    conclusiones = f"""
=== 8. Conclusiones del Incidente ===
- El sistema atravesó un momento crítico exactamente en la ventana que inicia a las {critical_moment_start}.
- El servicio más afectado en este periodo fue "{top_service}", siendo responsable absoluto de los fallos.
- La zona del sistema más comprometida fue el endpoint "{top_endpoint}".
- El problema originario dominó los registros con el mensaje de error: "{top_message}".
- Al comparar el incidente con el baseline general, la tasa de errores (bad_rate) pasó dramáticamente de un {base_bad_rate:.1f}% normal hacia un preocupante {inc_bad_rate:.1f}%.
    """
    print(conclusiones)

except Exception as e:
    print(f"Error en la ejecución o archivo no encontrado: {e}")

