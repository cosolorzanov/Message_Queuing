import time

from pg import DB
from multiprocessing import Pool


def read_prop(filename):
    props = {}
    with open(filename, 'r') as f:
        text = f.read().split('\n')
    for i in text:
        if len(i) == 0:
            continue
        tmp = i.split('=')
        props[tmp[0]] = tmp[1]
    return props


class GenericProducer:
    producer = None

    def __init__(self, props):
        self.producer = ProducerQueuePostgreSQLClient(props)

    def send(self, topic, key, value):
        self.producer.send(topic, key, value)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()


class GenericRecord:

    def __init__(self, id, topic, key, value):
        self.id = id
        self.topic = topic
        self.key = key
        self.value = value


class GenericConsumer:
    consumer = None

    def __init__(self, props, topic):
        self.topic = topic
        self.consumer = ConsumerQueuePostgreSQLClient(props, topic, 100)

    def listen(self, func):
        print('Listening messages')
        self.consumer.foreach_batch_and_process_item(func, 0.5, True)

    def close(self):
        self.consumer.close()


class ProducerQueuePostgreSQLClient:
    tableName = "queue_table"
    conn = None

    def __init__(self, conf):
        self.conn = DB(dbname=conf["postgresql.database"], host=conf["postgresql.server"],
                       port=int(conf["postgresql.port"]),
                       user=conf["postgresql.user"], passwd=conf["postgresql.password"])

    def send(self, topic, id, data: [bytes]):
        self.conn.query("INSERT INTO %s (topic, topic_id, data) VALUES ('%s','%s',$$%s$$);" % (
            self.tableName, topic, id, data.decode('utf-8')))

    def flush(self):
        self.tableName

    def close(self):
        self.conn.close()


class ConsumerQueuePostgreSQLClient:
    LIMIT = 10
    tableName = "queue_table"
    topic = None
    conn = None

    def __init__(self, conf, topic, size_batch):
        self.topic = topic
        self.LIMIT = size_batch
        self.conn = DB(dbname=conf["postgresql.database"], host=conf["postgresql.server"],
                       port=int(conf["postgresql.port"]),
                       user=conf["postgresql.user"], passwd=conf["postgresql.password"], )

    def __begin(self):
        self.conn.begin()

    def __get_batch(self):
        result = []
        q = self.conn.query(" SELECT * FROM " + self.tableName + " WHERE topic='" + self.topic + "' LIMIT " + str(
            self.LIMIT) + " FOR UPDATE SKIP LOCKED;")

        dict_result = q.dictresult()
        for row in dict_result:
            result.append(GenericRecord(row["id"], row["topic"], row["topic_id"], row["data"]))
        return result

    def __delete_batch(self, result: [GenericRecord]):
        ids = []
        if len(result) > 0:
            for row in result:
                ids.append("id=" + str(row.id))
            self.conn.query("DELETE FROM " + self.tableName + " WHERE ( " + str.join(" OR ", ids) + " );")

    def __commit(self):
        self.conn.commit()

    def __rollback(self):
        self.conn.rollback()

    def foreach_batch_and_process_item(self, func, sleep: float = 0.5, commit: bool = True):
        while True:
            try:
                self.__begin()
                batch = self.__get_batch()
                if batch is not None and len(batch) > 0:
                    with Pool(processes=10) as p:
                        p.map(func, batch)
                self.__delete_batch(batch)
                self.__commit() if commit else None
            except NameError:
                self.__rollback()
                print(NameError.name)
            time.sleep(sleep)

    def close(self):
        self.conn.close()


##EXAMPLE. Using 100 rows for each batch. Into ConsumerQueuePostgreSQLClient.foreach_batch_and_process_item you can change the number of processes
# def listen_queue_postgres():
#     consumer_postgres = ConsumerQueuePostgreSQLClient(props, "topic_to_consumer", 100)
#     consumer_postgres.foreach_batch_and_process_item(process_record, 0.5, True)


# props = {}
# read_prop("properties.conf")
# listen_queue_postgres()

# pp = ProducerQueuePostgreSQLClient(props)
# pp.send("topic_to_producer", "test", "data test")
# pp.close()
