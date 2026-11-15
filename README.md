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

| Área | Casos que pasaron | Compatibilidad pendiente |
| --- | --- | --- |
| Colas estándar y FIFO | Ciclo de mensajes, retraso y visibilidad, paginación, vaciado, etiquetas, aislamiento regional, deduplicación y reintentos FIFO. | Respuesta HTTP para cargas grandes y atributo del sistema `SenderId`. |
| Protocolos | Los clientes Query/XML y JSON comparten estado. Pasaron los casos de envío, recepción y errores XML ejercitados por la suite. | Quedan casos de interoperabilidad que usan IAM y Lambda. |
| DLQ | Traslado al superar el máximo de recepciones, políticas de redrive, listado de orígenes y devolución y cancelación de tareas. | Casos de configuración desde CloudFormation o asociados a una clave KMS. |
| Atributos | Ida y vuelta de `String`, `Number` y `Binary`, MD5, selección por prefijo y validaciones de nombre, tipo y número. | El tratamiento de algunas cabeceras `AWSTraceHeader` malformadas. |
| Políticas e integraciones | Identidad local mediante `STS.GetCallerIdentity`. | Políticas de cola, permisos, IAM, KMS, Lambda, CloudFormation y CloudWatch. |
