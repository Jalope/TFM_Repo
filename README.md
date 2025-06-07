📊 **Análisis generacional con datos públicos - TFM**

Este repositorio contiene el desarrollo del Trabajo de Fin de Máster centrado en el análisis de las desigualdades intergeneracionales mediante el uso de tecnologías open source para la ingesta, almacenamiento y visualización de datos públicos.

### 🎯 Objetivo

Construir una plataforma que permita analizar cómo afectan las políticas públicas a las generaciones jóvenes en comparación con cohortes mayores (baby boomers, generación X), abordando temáticas como vivienda, demografía y pensiones.

### ⚙️ Arquitectura

El sistema se compone de:

- **Kafka** para la ingesta distribuida de datos desde fuentes públicas (INE, Eurostat, Seguridad Social...).
- **MongoDB** como base de datos documental para el almacenamiento flexible.
- **Metabase** para la creación de dashboards y visualización de KPIs.
- **Kubernetes** para el despliegue orquestado de servicios.

### 📁 Contenido del repositorio

- Scripts de extracción y procesamiento de datos.
- Definición de esquemas canónicos por dominio (vivienda, demografía, pensiones...).
- Configuración de infraestructura como código (Helm charts, CI/CD).
- Dashboards construidos y narrativa de análisis.