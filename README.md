# Ginistack

Emulador local de AWS escrito en Go 1.27. Incluye colas estándar y FIFO de SQS y usa solo la biblioteca estándar de Go. El método genérico `router.register[Input, Output]` conecta cada operación tipada con el protocolo AWS JSON 1.0.

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

- Crear, resolver y borrar colas estándar y FIFO, leer la configuración de recepción y listar las etiquetas guardadas al crear la cola y consultar el número de mensajes disponibles, retrasados y en vuelo.
- Enviar, recibir y borrar mensajes, conservar el cuerpo y su MD5, programar el retraso inicial y cambiar el tiempo de visibilidad. Al recibir, se pueden pedir el contador de entregas y las marcas de tiempo de envío y primera recepción.
- Enviar, borrar y cambiar la visibilidad por lotes de hasta 10 entradas, con resultados por entrada. El total de cuerpos y atributos personalizados de un lote de envío admite hasta 1 MiB.
- Esperar mensajes con *long polling* y volver a entregar los que recuperan la visibilidad.

- Orden FIFO por grupo, deduplicación explícita o por contenido durante cinco minutos y repetición de recepciones mediante `ReceiveRequestAttemptId`.
- Traslado de mensajes a una DLQ del mismo tipo al intentar superar el límite de recepciones del origen. Se conservan el identificador, cuerpo, atributos personalizados y cabecera de trazas.
- Atributos de mensaje `String`, `Number` y `Binary`, con MD5 y selección por nombre o prefijo en recepción.

El estado se guarda en memoria y se pierde al detener el proceso. Se usa una cuenta local fija y el servidor acepta credenciales de prueba sin verificar SigV4. El protocolo Query, la gestión de tareas de devolución desde la DLQ y las políticas de autorización de redrive quedan pendientes.

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

El modo completo limita cada caso a 45 segundos por defecto. Puedes aumentar ese presupuesto con `GINISTACK_CASE_TIMEOUT` para comprobar casos que requieren esperar varios minutos en tiempo real.

### Resultados de SQS

La ejecución del 15 de noviembre de 2026 probó Ginistack `4bc0cd9` con Go 1.27.4 y la [suite oficial de AWS en la revisión `4df2823`](https://github.com/aws/aws-emulator-evaluation-suite/tree/4df2823bafface2ada35f482c07ff6668cc078e1). Pasaron **132 de 306 casos (43,1 %)**, incluidos los **31 de la selección de regresión**.

| Resultado del runner | Casos | Qué ocurrió |
| --- | ---: | --- |
| Aprobado | 132 | Las comprobaciones del caso se cumplieron. |
| Comprobación fallida | 60 | Hubo diferencias de comportamiento, de código o de texto de error. Un caso que invoca AWS CLI desde un script también quedó en este grupo porque faltaba `aws`. |
| Sin implementar | 53 | El caso dependía de una operación, un atributo de cola o un servicio aún pendiente. |
| Error de la API | 49 | Una llamada devolvió un error inesperado. Aquí entran casos de Query y flujos que dependen de IAM, KMS o CloudFormation. |
| Tiempo agotado | 8 | El caso superó el límite de 45 segundos. |
| Herramienta ausente | 4 | El runner detectó que faltaba AWS CLI. |

| Área | Casos que pasaron | Compatibilidad pendiente |
| --- | --- | --- |
| Colas estándar | Crear, enviar, recibir y borrar. *Long polling*, reentrega por visibilidad y fallos parciales en operaciones por lotes. | `ListQueues`, `PurgeQueue`, gestión de etiquetas y coincidencia de varios errores de la API. |
| FIFO | Orden por grupo con receptores concurrentes, deduplicación por ID y por contenido, aislamiento entre grupos y repetición de una recepción con `ReceiveRequestAttemptId`. | Rechazo de lotes FIFO con ciertas entradas inválidas y rechazo del reintento tras modificar el lote original. |
| DLQ | Traslado automático tras alcanzar el límite de recepciones y conservación de ID, cuerpo, atributos y trazas. El traslado también desbloquea el grupo FIFO de origen. | Devolución de mensajes desde la DLQ, políticas `RedriveAllowPolicy`, listado de colas de origen y aislamiento entre regiones. |
| Atributos de mensaje | Ida y vuelta de `String`, `Number` y `Binary`, cálculo del MD5 y selección por nombre o prefijo. | Validación estricta de nombres, tipos y valores numéricos. |

La prueba de retención en DLQ pasó en **87 segundos** al repetirla con un límite de 110 segundos. Su resultado original sigue en la fila de tiempo agotado. La prueba del vencimiento de la ventana de deduplicación FIFO quedó registrada como fallo, pero requiere observar cinco minutos y la pasada de 45 segundos dejó ese comportamiento sin confirmar.
