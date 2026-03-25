# Log Analysis Challenge - The Huddle Challenge 6

## Descripción del Proyecto
Este proyecto consiste en el análisis de un sistema de logging distribuido para identificar y diagnosticar incidentes críticos. Utilizando un dataset de logs en formato CSV (`server_logs.csv`), el objetivo es extraer evidencia cuantitativa sobre el comportamiento del sistema, detectando ventanas de tiempo con altas tasas de error (bad rate), servicios más afectados y endpoints comprometidos.

El análisis se centra en:
- Exploración inicial de severidades y servicios.
- Detección del **momento crítico** (ventana de 5 minutos con mayor tasa de error).
- Diagnóstico detallado del incidente (mensajes de error, latencia y endpoints).
- Comparación entre el periodo de incidente y el estado base (baseline).
- Visualización temporal de eventos por severidad y tasa de errores.

## Requisitos y Herramientas
Para ejecutar este proyecto, se requiere tener instalado **Python** y las siguientes librerías:

- **Pandas**: Para la manipulación y análisis de datos.
- **Matplotlib**: Para la generación de gráficos y visualizaciones.
- **Jupyter Notebook**: Entorno necesario para correr el análisis de forma interactiva y reproducible.

## Pasos para su Utilización
Sigue estos pasos para ejecutar el análisis:

1. **Preparar el entorno**:
   Se recomienda crear un entorno virtual e instalar las dependencias necesarias:
   ```bash
   pip install pandas matplotlib notebook
   ```

2. **Cargar los datos**:
   Asegúrate de que el archivo `server_logs.csv` se encuentre dentro de la carpeta `docs/`.

3. **Ejecutar el análisis**:
   Lanza Jupyter Notebook en la raíz del proyecto:
   ```bash
   jupyter notebook
   ```
   Abre el archivo `challenge_6.ipynb` y ejecuta todas las celdas de arriba hacia abajo para generar los reportes y gráficos obligatorios.

4. **Revisar resultados**:
   Al final del notebook encontrarás las conclusiones detalladas sobre el momento crítico, el servicio más afectado y los cambios detectados durante el incidente.

---
**Realizado por:** Edgar Vega
