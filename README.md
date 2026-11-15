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
