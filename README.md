# Ginistack

Emulador local de AWS escrito en Go 1.27. Incluye SQS y SNS y usa solo la biblioteca estándar de Go. SQS usa el método genérico `Handler.register[Input, Output]` para compartir operaciones entre AWS JSON 1.0 y AWS Query/XML. SNS usa AWS Query/XML en el mismo puerto.

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

Para crear un tópico local de SNS en el mismo servidor:

```sh
export AWS_ENDPOINT_URL_SNS=http://127.0.0.1:4566
topic_arn=$(aws sns create-topic --name orders --query TopicArn --output text)
aws sns list-topics
```

## Alcance

### SQS

- Crear, resolver, listar y borrar colas estándar y FIFO por región. La lista admite prefijos y paginación. Se pueden vaciar colas, gestionar sus etiquetas y consultar los mensajes disponibles, retrasados y en vuelo.
- Enviar, recibir y borrar mensajes, conservar el cuerpo y su MD5, programar el retraso inicial y cambiar el tiempo de visibilidad. Al recibir, se pueden pedir el contador de entregas y las marcas de tiempo de envío y primera recepción.
- Enviar, borrar y cambiar la visibilidad por lotes de hasta 10 entradas, con resultados por entrada. El total de cuerpos y atributos personalizados de un lote de envío admite hasta 1 MiB.
- Esperar mensajes con *long polling* y volver a entregar los que recuperan la visibilidad.

- Orden FIFO por grupo, deduplicación explícita o por contenido durante cinco minutos y repetición de recepciones mediante `ReceiveRequestAttemptId`.
- Traslado de mensajes a una DLQ del mismo tipo al intentar superar el límite de recepciones del origen. Se conservan el identificador, cuerpo, atributos personalizados y cabecera de trazas. Las políticas de redrive controlan qué orígenes pueden usar la DLQ. Las tareas de devolución se pueden iniciar, listar y cancelar.
- Atributos de mensaje `String`, `Number` y `Binary`, con MD5 y selección por nombre o prefijo en recepción.
- Cifrado SSE-SQS del cuerpo en memoria con AES-256-GCM y una clave local por proceso. `SqsManagedSseEnabled` permite configurarlo al crear o actualizar la cola. Los cambios afectan a los nuevos envíos y los mensajes ya cifrados conservan el cifrado mientras están en cola, incluso después de un redrive.
- Políticas de cola con lectura, edición y borrado mediante `Policy`. `AddPermission` y `RemovePermission` modifican el mismo documento por `Sid`. El evaluador de `internal/policy` aplica un `Deny` explícito a `SendMessage` y `SendMessageBatch` para el principal local.

El estado se guarda en memoria y se pierde al detener el proceso. Se usa una cuenta local fija y el servidor acepta credenciales de prueba sin verificar SigV4. Quedan pendientes las políticas de identidad de IAM, la autorización de otras operaciones y las integraciones con KMS, CloudFormation y CloudWatch.

### SNS

- Crear, listar y borrar tópicos estándar y FIFO por región, con paginación, atributos y etiquetas. Borrar el tópico elimina sus suscripciones.
- Suscribir colas SQS, listar y consultar suscripciones, actualizar `RawMessageDelivery` y filtrar por igualdad de atributos `String` y `Number`.
- Publicar hacia una cola SQS en modo raw o con el envoltorio de SNS. Los tópicos FIFO mantienen el orden por grupo y deduplican por ID o por contenido. La cola debe autorizar a `sns.amazonaws.com` a ejecutar `sqs:SendMessage` mediante su política. El evaluador compartido comprueba el ARN del tópico y aplica las denegaciones de la cola.
- El envoltorio de los tópicos estándar incluye una firma verificable con el certificado local servido por Ginistack. El certificado y el estado de SNS duran lo que dura el proceso.

Quedan pendientes `PublishBatch`, otros operadores y ámbitos de filtrado, suscripciones a otros protocolos e integraciones con otros servicios.

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

Para probar SNS y su entrega a SQS:

```sh
./tests/fidelity.sh --sns
```

Para cubrir los casos de línea de comandos, instala AWS CLI v2 y déjala disponible en `PATH`. El runner oficial asigna 60 segundos por defecto y hasta 600 para los casos marcados como de tiempo real o que requieren herramientas lentas. Puedes fijar un límite distinto con `GINISTACK_CASE_TIMEOUT`.

### Resultados de SQS

La ejecución del 15 de noviembre de 2026 probó Ginistack `3080be3` con Go 1.27.4, AWS CLI v2.40.3 y la [suite oficial de AWS en la revisión `4df2823`](https://github.com/aws/aws-emulator-evaluation-suite/tree/4df2823bafface2ada35f482c07ff6668cc078e1). Pasaron **254 de 306 casos (83,0 %)**, incluidos los **45 de la selección de regresión**. Pasaron 112 casos más y se conservaron los 142 aprobados de la referencia.

| Resultado del runner | Referencia | Pasada actual |
| --- | ---: | ---: |
| Aprobado | 142 | 254 |
| Comprobación fallida | 58 | 4 |
| Sin implementar | 54 | 39 |
| Error de la API | 50 | 8 |
| Tiempo agotado | 2 | 1 |
| **Total** | **306** | **306** |

El único caso que agotó su presupuesto oficial de 60 segundos verifica un recibo de una cola borrada y recreada. Pasó en **63 segundos** con un presupuesto ampliado, lo que da **255 de 306** si se incluye esa repetición.

Tras esa pasada se añadieron a la regresión dos casos sobre `AWSTraceHeader` y `SenderId`, diez sobre permisos y políticas de cola, dos sobre tamaño y uno sobre SSE-SQS. La selección actual pasa **60 de 60**. Al combinar la pasada completa y las verificaciones posteriores, incluidos los 63 segundos del caso de la cola recreada, hay **269 de 306 casos aprobados**. Se aplazan los **37 casos pendientes** que requieren otros servicios. La tabla conserva el resultado de la última ejecución completa.

SQS devuelve HTTP `413` con el cuerpo de respuesta vacío si el cuerpo de la solicitud supera los 1.638.400 bytes, el límite observado en AWS Emulator. Las solicitudes que caben en ese límite siguen sujetas al tamaño de mensaje configurado en la cola, que por defecto es de 1 MiB.

| Área | Casos que pasaron | Compatibilidad pendiente |
| --- | --- | --- |
| Colas estándar y FIFO | Ciclo de mensajes, retraso y visibilidad, paginación, vaciado, etiquetas, aislamiento regional, deduplicación y reintentos FIFO. `SenderId` identifica al principal local. Límite HTTP `413` y límite de mensaje de 1 MiB. | |
| Protocolos | Los clientes Query/XML y JSON comparten estado. Pasaron los casos de envío, recepción y errores XML ejercitados por la suite. | Quedan casos de interoperabilidad que usan IAM y Lambda. |
| DLQ | Traslado al superar el máximo de recepciones, políticas de redrive, listado de orígenes y devolución y cancelación de tareas. | Casos de configuración desde CloudFormation o asociados a una clave KMS. |
| Atributos | Ida y vuelta de `String`, `Number` y `Binary`, MD5, selección por prefijo y validaciones de nombre, tipo y número. Las trazas válidas se conservan, las vacías se rechazan y las de tipo o formato inválido se descartan al entregar. | |
| Cifrado | SSE-SQS conserva byte por byte el cuerpo y los atributos del mensaje. | Resolución de claves y permisos de SSE-KMS. |
| Políticas e integraciones | Identidad local mediante `STS.GetCallerIdentity`, gestión de políticas de cola y permisos por `Sid`. Un `Deny` explícito bloquea el envío. | Evaluación de políticas de identidad y autorización de otras operaciones. El caso oficial de límites de políticas requiere crear usuarios en IAM. Quedan KMS, Lambda, CloudFormation y CloudWatch. |

### Resultados de SNS

La primera selección de regresión usa la misma revisión oficial. Cubre el ciclo de vida de tópicos y suscripciones, publicación, tamaño de mensajes, FIFO, formato y firma del envoltorio, filtrado y entrega a SQS con políticas de cola.

| Directorio de la suite | Casos aprobados en la selección | Casos del directorio |
| --- | ---: | ---: |
| `sns` | 45 | 245 |
| `sns__sqs` | 6 | 12 |
| **Total** | **51** | **257** |

El caso que verifica que ampliar un filtro no recupera mensajes descartados usa un presupuesto de 120 segundos para cubrir dos ventanas de ausencia. Los otros 206 casos de esos directorios quedan pendientes de evaluar en una ejecución completa.
