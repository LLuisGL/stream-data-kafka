
# Como ejecutar el proyecto (Por modulos)

  # IMPORTANTE ANTES DE EMPEZAR

Para que salga todo bien, verifiquen siempre los archivos ya que en la mayoria hay rutas y puede que las que haya colocado en mi computadora no sean las mismas a las de ustedes, esto es importante porque de otra manera no les funcionará la aplicación.
	

## Módulo Kafka (Generación de Datos)

Primero se debe de elvantar todo el apartado de kafka, para eso primero hay que ir al kafka instalado (3.8.0) a la carpeta bin y colocar en terminal los siguientes comando
- `./zookeeper-server-start.sh ../config/zookeeper.properties`
- `./kafka-server-start.sh ../config/server.properties`

A continuación se crean los 3 tópicos que se usaran del servicio E-Commerce

- `./kafka-topics.sh --create --bootstrap-server  localhost:9092  --replication-factor  1  --partitions  1  --topic  cart_events`

- `./kafka-topics.sh --create --bootstrap-server  localhost:9092  --replication-factor  1  --partitions  1  --topic  orders`

- .`/kafka-topics.sh --create --bootstrap-server  localhost:9092  --replication-factor  1  --partitions  1  --topic  page_views`

Con esto vamos a tener levantado ya el módulo para Kafka, podríamos ejecutar nuestro archivo .py en la carpeta "data-faker" dentro de nuestro repositorio

## Módulo Spark y HDFS (Ingesta De Datos)

Para este módulo es necesario levantar Hadoop, ya depende de como lo hayan instalado, en mi caso lo tengo en un usuario aparte, por lo que me tocó entrar allí y ejecutar "./start-dfs.sh" dentro de la carpeta "sbin" para poder levantar el DFS dentro de hadoop, dependerá mucho de sus instalaciones y las complicaciones que puedan tener (más abajo explico en un apartado los errores que tuve por si tienen similares). 

También deberemos de crear las carpetas donde se alojarán nuestros parquets, para eso en hdfs colocamos:

- hdfs dfs -mkdir -p /user/**[mi-usuario]**/ecommerce/

Notese que la ruta puede ser la que mejor les convenga y no es necesario crear las carpetas para "orders", "page_views" y "cart_events" ya que estas se crearan automáticamente cuando se ejecute el programa

 Con esto nos podemos dirigir a Spark y colocar el siguiente comando:

- spark-submit --jars /home/**[mi-usuario]**/spark-kafka-jars/spark-sql-kafka-0-10_2.12-3.5.7.jar,/home/**[mi-usuario]**/spark-kafka-jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar,/home/luis-goncalves/spark-kafka-jars/kafka-clients-3.6.1.jar,/home/**[mi-usuario]**/spark-kafka-jars/commons-pool2-2.12.0.jar /home/**[mi-usuario]**/Desktop/PDM/stream-data-kafka/data-process/spark_stream_orders.py 

Reemplazar **[mi-usuario]** con el usuario donde se ubican esas rutas

> NOTA 1: la carpeta "spark-kafka-jars" fue una carpeta creada por mi donde descargue esas dependencias ya que descargarlas por Maven me daba errores

> NOTA 2: La última ruta corresponde a donde tenemos nuestro archivo .py de spark, que en mi caso era allí

Si todo salió correcto, deberíamos de tener el programa corriendo y soltando algunos mensajes nada importantes, ahora deberemos de abrir una consola nueva y abrir "spark-shell", una vez abierto si hicimos todos los pasos correctos colocando el siguiente comando podríamos ver los datos guardados en HDFS:

- spark.read.parquet("hdfs://localhost:9000/user/**[mi-usuario]**/ecommerce/orders/fecha=2025-12-15").show(false)

Notar que coloque la fecha del 15 de diciembre de 2025, si está en otra fecha deberia de cambiarse eso también.

>NOTA: Los datos no sueles aparecer tan rapidos, suelen demorar como máximo 3 minutos para poder mostrar los primeros, luego de eso si empezará a actualizarlos más seguidos.

Con esto ya tendremos el módulo de HDFS levantado

## Módulo de Visualización

Para este módulo es necesario instalar los requirements nuevos que coloqué en el repositorio, luego de esto debemos activar nuestro entorno virtual y colocar el siguiente comando:

 -  streamlit run /home/luis-goncalves/Desktop/PDM/stream-data-kafka/data-visualization/dash.py

**IMPORTANTE**: En el caso de saltar errores probar a colocar estos comandos

- `export  HADOOP_HOME=/home/hadoop/hadoop-3.4.0`  
- `export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop`  
- `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`  
- `export CLASSPATH="$(hadoop classpath --glob 2>/dev/null)"`  
- `export LD_LIBRARY_PATH= $HADOOP_HOME/lib/native: $LD_LIBRARY_PATH`  
- `export ARROW_LIBHDFS_DIR=$HADOOP_HOME/lib/native`

Con esto posiblemente se solucione los problemas y podamos visualizar los gráficos a tiempo real.

# Problemas Comunes

## Problemas con Hadoop

Los problemas que experimenté con Hadoop fueron varios ya que para mi instalación yo cree un usuario exclusivo para Hadoop, por lo que tuve que darle accesos de escritura a mi usuario Main (luis-goncalves) para que pudiera crear carpetas en HDFS y demás, esto es importante ya que donde se ejecuta el programa debe de estar esas carpetas, de otra manera les dara errores de inaccesibilidad

## Problemas con Spark

Tuve errores con Spark a la hora de ejecutar "spark-submit" debido a que no me dejaba descargar esas dependencias que me pedia, por lo que tuve que optar a descargarlas con w-get una por una y luego juntarlas en una carpeta para hacer ese script que estaba de los primeros

## Otros problemas a considerar (Y solucionar si se puede)

A veces la primera vez que se deben de generar los parquets demora un montón de tiempo no lo sé por qué. También a la hora de visualizar los datos puede que a veces tarden un poco más de lo normal en renderizar y muestre comportamientos raros.


