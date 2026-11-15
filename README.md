# Ginistack

Emulador local de AWS escrito en Go 1.27. Incluye colas estándar y FIFO de SQS y usa solo la biblioteca estándar de Go. El método genérico `router.register[Input, Output]` conecta las operaciones tipadas con AWS JSON 1.0 y AWS Query/XML sobre el mismo estado.

## Arranque

```sh
go run ./cmd/ginistack
```

El servidor escucha en `http://127.0.0.1:4566`. Puedes probarlo con AWS CLI v2.

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-west-2
export AWS_ENDPOINT_URL_SQS=http://127.0.0.1:4566
queue_url=$(aws sqs create-queue --queue-name orders --query QueueUrl --output text)
aws sqs send-message --queue-url "$queue_url" --message-body 'hola'
aws sqs receive-message --queue-url "$queue_url" --wait-time-seconds 2
```

## Alcance

- Crear, resolver, listar y borrar colas estándar y FIFO por región. La lista admite prefijos y paginación. Se pueden vaciar colas, gestionar sus etiquetas y consultar los mensajes disponibles, retrasados y en vuelo.
- Enviar, recibir y borrar mensajes, conservar el cuerpo y su MD5, programar el retraso inicial y cambiar el tiempo de visibilidad. Al recibir, se pueden pedir el contador de entregas y las marcas de tiempo de envío y primera recepción.
- Enviar, borrar y cambiar la visibilidad por lotes de hasta 10 entradas, con resultados por entrada. El total de cuerpos y atributos personalizados de un lote de envío admite hasta 1 MiB.
- Esperar mensajes con *long polling* y volver a entregar los que recuperan la visibilidad.

- Orden FIFO por grupo, deduplicación explícita o por contenido durante cinco minutos y repetición de recepciones mediante `ReceiveRequestAttemptId`.
- Traslado de mensajes a una DLQ del mismo tipo al intentar superar el límite de recepciones del origen. Se conservan el identificador, cuerpo, atributos personalizados y cabecera de trazas. Las políticas de redrive controlan qué orígenes pueden usar la DLQ. Las tareas de devolución se pueden iniciar, listar y cancelar.
- Atributos de mensaje `String`, `Number` y `Binary`, con MD5 y selección por nombre o prefijo en recepción.

El estado se guarda en memoria y se pierde al detener el proceso. Se usa una cuenta local fija y el servidor acepta credenciales de prueba sin verificar SigV4. Las políticas de acceso a colas y las integraciones con IAM, KMS, CloudFormation y CloudWatch quedan pendientes.

## Pruebas oficiales de AWS

El script requiere Go 1.27, un compilador C para el detector de carreras, `git`, `curl`, `uv` y Python 3.12.

```sh
./tests/fidelity.sh
```

El script arranca Ginistack, ejecuta una selección de casos de SQS del repositorio `aws/aws-emulator-evaluation-suite` en `us-west-2` y guarda los resultados por caso en `.fidelity-reports/`. Usa el commit fijado en el script y admite `AWS_SUITE_DIR` para reutilizar un checkout local de esa revisión.

Para ejecutar el directorio completo de SQS y ver también qué casos quedan pendientes:

```sh
./tests/fidelity.sh --all-sqs
```

Para cubrir los casos de línea de comandos, instala AWS CLI v2 y déjala disponible en `PATH`. El runner oficial asigna 60 segundos por defecto y hasta 600 para los casos marcados como de tiempo real o que requieren herramientas lentas. Puedes fijar un límite distinto con `GINISTACK_CASE_TIMEOUT`.

### Resultados de SQS

La ejecución del 15 de noviembre de 2026 probó Ginistack `460e03d` con Go 1.27.4, AWS CLI v2.40.3 y la [suite oficial de AWS en la revisión `4df2823`](https://github.com/aws/aws-emulator-evaluation-suite/tree/4df2823bafface2ada35f482c07ff6668cc078e1). Pasaron **142 de 306 casos (46,4 %)**, incluidos los **31 de la selección de regresión**.

| Resultado del runner | Pasada inicial · 45 s, sin AWS CLI | Pasada con AWS CLI y tiempos oficiales |
| --- | ---: | ---: |
| Aprobado | 132 | 142 |
| Comprobación fallida | 60 | 58 |
| Sin implementar | 53 | 54 |
| Error de la API | 49 | 50 |
| Tiempo agotado | 8 | 2 |
| Herramienta ausente | 4 | 0 |
| **Total** | **306** | **306** |

El emulador era el mismo en ambas pasadas. Con AWS CLI disponible pasaron tres pruebas que dependían de ella. Con los tiempos oficiales también pasaron seis casos que antes agotaban su plazo, incluida la retención en DLQ, y se verificó que un ID FIFO se puede reutilizar después de que venza la ventana de deduplicación de cinco minutos.

Quedaron dos casos con el tiempo agotado. Uno comprueba que cambiar el retraso de una cola afecta de forma distinta a los mensajes ya encolados en FIFO y en estándar. El otro verifica el ciclo de un mensaje y el error de la API después de borrar su cola.

| Área | Casos que pasaron | Compatibilidad pendiente |
| --- | --- | --- |
| Colas estándar | Crear, enviar, recibir y borrar. *Long polling*, reentrega por visibilidad y fallos parciales en operaciones por lotes. | `ListQueues`, `PurgeQueue`, gestión de etiquetas y coincidencia de varios errores de la API. |
| FIFO | Orden por grupo con receptores concurrentes, deduplicación por ID y por contenido, aislamiento entre grupos y repetición de una recepción con `ReceiveRequestAttemptId`. | Rechazo de lotes FIFO con ciertas entradas inválidas y rechazo del reintento tras modificar el lote original. |
| DLQ | Traslado automático tras alcanzar el límite de recepciones y conservación de ID, cuerpo, atributos y trazas. El traslado también desbloquea el grupo FIFO de origen. | Devolución de mensajes desde la DLQ, políticas `RedriveAllowPolicy`, listado de colas de origen y aislamiento entre regiones. |
| Atributos de mensaje | Ida y vuelta de `String`, `Number` y `Binary`, cálculo del MD5 y selección por nombre o prefijo. | Validación estricta de nombres, tipos y valores numéricos. |
