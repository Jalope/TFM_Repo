**Guía de despliegue de infraestructura (MongoDB + Metabase con Docker Compose)**

1. **Prerequisitos**

   - Windows con **Docker Desktop** instalado y en marcha.
   - (Opcional) **MongoDB Compass**, sólo si quiere inspeccionar datos localmente.

2. **Crear carpeta del proyecto**

   ```powershell
   cd C:\Users\TuUsuario\Documents
   mkdir tfm-bienvenida
   cd tfm-bienvenida
   ```

3. **Definir `docker-compose.yml`**
    En `tfm-bienvenida\docker-compose.yml`, pegar:

   ```yaml
   version: '3.8'
   
   services:
     metabase:
       image: metabase/metabase:latest
       container_name: tfm_metabase
       restart: unless-stopped
       ports:
         - "3000:3000"
       volumes:
         - metabase_data:/metabase.db
       depends_on:
         - mongo
       networks:
         - backend
   
     mongo:
       image: mongo:latest
       container_name: tfm_mongo
       restart: unless-stopped
       environment:
         MONGO_INITDB_ROOT_USERNAME: jalope
         MONGO_INITDB_ROOT_PASSWORD: admin
         MONGO_INITDB_DATABASE: tfm_db
       ports:
         - "27017:27017"
       volumes:
         - mongo_data:/data/db
       networks:
         - backend
   
   volumes:
     metabase_data:
     mongo_data:
   
   networks:
     backend:
       driver: bridge
   ```

4. **Arrancar los servicios**

   ```powershell
   docker-compose down -v   # Elimina volumenes antiguos
   docker-compose up -d     # Levanta MongoDB y Metabase
   ```

   Verificar:

   ```powershell
   docker ps
   ```

   Deben aparecer `tfm_mongo` (puerto 27017) y `tfm_metabase` (puerto 3000).

5. **Crear usuario y base de datos en MongoDB**

   1. Entrar al shell de Mongo:

      ```powershell
      docker exec -it tfm_mongo mongosh
      ```

   2. En el prompt `test>` ejecutar:

      ```js
      use admin
      db.createUser({
        user: "jalope",
        pwd:  "admin",
        roles: [ { role: "root", db: "admin" } ]
      })
      use tfm_db
      db.createCollection("mi_coleccion_prueba")
      ```

   3. Salir con `exit`.

6. **Insertar documento de prueba**

   1. Volver a entrar con `docker exec -it tfm_mongo mongosh --username jalope --password admin --authenticationDatabase admin`.

   2. En el prompt:

      ```js
      use tfm_db
      db.mi_coleccion_prueba.insertOne({
        nombre: "Elemento de prueba",
        valor: 1
      })
      ```

   3. Comprobar:

      ```js
      db.mi_coleccion_prueba.find().pretty()
      ```

   4. Salir con `exit`.

7. **Configurar Metabase**

   1. Abrir navegador en `http://localhost:3000`.

   2. Completar el asistente hasta **“Add your database”**.

   3. Seleccionar **MongoDB** → **Pegar una cadena de conexión**.

   4. Copiar exactamente:

      ```
      mongodb://jalope:admin@mongo:27017/tfm_db?authSource=admin&directConnection=true
      ```

   5. Desactivar **SSL** y **SSH** → **Test & Save** → debe mostrar **Connection successful**.

8. **Verificar y crear primer dashboard**

   1. En Metabase, ir a **Browse data → tfm_db → mi_coleccion_prueba** para ver el documento de prueba.
   2. Crear una **Question** tipo **Bar chart**:
      - Eje X: `nombre`
      - Métrica: `valor` (suma o media)
   3. Guardar la pregunta y añadirla a un **Dashboard**.

------

Con estos pasos tendrás toda la infraestructura montada, la base de datos con un registro de prueba y Metabase conectado y listo para construir dashboards.