import sys

from sub_pub import read_prop, GenericProducer, GenericConsumer, GenericRecord


def process(text):
    if len(text) == 0:
        return {}
    ## your code
    d = {'dates': set(), 'per': set(), 'org': set(), 'loc': set(), 'misc': set()}
    return d


# Here you can implement the function to process each row  (message)
def process_record(record: GenericRecord):
    key = record.key
    value = record.value
    print('Processing', key)
    d = process(value)
    result = []
    for i in d:
        result.append(i + ':=' + '|'.join(d[i]))
    try:
        producer.send(sys.argv[3], key=key, value=','.join(result).encode("utf-8"))
        producer.flush()
        print(key + ' processed')
    except Exception as e:
        print('Error sending message')
        print(e)
    print('Text processed')


def listen():
    consumer.listen(process_record)


if len(sys.argv) != 4:
    print(len(sys.argv))
    print(sys.argv)
    print("bad arguments: properties_conf topic_to_consumer topic_to_producer")
    exit(2)

props = read_prop(sys.argv[1])
print(props)


producer = GenericProducer(props)
consumer = GenericConsumer(props, sys.argv[2])

try:
    listen()
except KeyboardInterrupt:
    print("closing")
    consumer.close()
    producer.flush()
    producer.close()
