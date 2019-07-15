# pulsarperformance
Is a utility to test the performance of a Pulsar installation. 

The code includes a producer and a consumer that are run independently.
## Consumer
The consumer can consume from multiple topics at a time. Every 10,000 messages received from a topic it logs out the count and the contents of the count field of the last message.

`java -cp pulsar-performance-1.0-SNAPSHOT.jar rik.direct.DirectConsumer <options>`

### options
| Short        | Long           | Required  | Description |
| --- | --- | --- | --- |
| -t      | --topics | true | Csv list of topics. All topic names are prepended with test- |
| -u      | --url      |   false | Service url for a broker/proxy. defaults to `pulsar://localhost:6650` |
| -h | --help      |    false | Displays help text |

## Producer
The Producer sends messages in json format containing a count field. The count is of the number of messages sent so far to that topic.
If a file is specified then larger messages and messages of varied sizes can be sent (See below for details).

`java -cp pulsar-performance-1.0-SNAPSHOT.jar rik.direct.DirectProducer <options>`
### options
| Short        | Long           | Required  | Description |
| --- | --- | --- | --- |
| -t      | --topics | true | Csv list of topics. All topic names are prepended with test- |
| -u      | --url      |   false | Service url for a broker/proxy. defaults to `pulsar://localhost:6650` |
| -h | --help      |    false | Displays help text |
| -r | --repetitions | false | number of messages to send to each topic. Defaults to 10,000 |
| -f | --filename | false | file to use as a template for messages |

### Template file format
The file format is in JSON and consists of an array of templates. Each template consists of 2 fields. The first field is the ratio of using this template vs the others in the array.
The second field contains the body of the message. The body must contain a field called `count` which must contain the string `${id}` which is used to hold the message number for use by the consumer's logging.

```
[
   {"ratio": 2, "body": {"field": "data", "count": "${id}"}},
   {"ratio": 2, "body": {"field": "data", "count": "${id}"}},
   {"ratio": 2, "body": {"field": "data", "count": "${id}"}},
   {"ratio": 2, "body": {"field": "data", "count": "${id}"}},
   {"ratio": 2, "body": {"field": "data", "count": "${id}"}}
 ]
```