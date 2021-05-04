from pymongo import MongoClient
import logging


class LogServer:
    def __init__(self):
        client = \
            MongoClient('mongodb+srv://new_user:<password>@gabrieldb.lrteu.mongodb.net/myFirstDatabase'
                        '?retryWrites=true&w=majority',
                        username='new_user',
                        password='new_user',
                        tls=True,
                        tlsAllowInvalidCertificates=True)
        self.logs_db = client.logs
        logging.basicConfig(filename='LogServer.log', level=logging.DEBUG, filemode='w', format='%(message)s')
        logging.info(f'task_id\t\t\ttime stamp\t\t\t\ttext')

    def add_log(self, task_id, time_stamp, text):
        entry = {
            '_id': time_stamp,
            'time_stamp': time_stamp,
            'text': text
        }
        try:
            self.logs_db[str(task_id)].insert_one(entry)
        except:
            print(f'task_id: {task_id}, time_stamp: {time_stamp} - already on the mongoDB')

    def generate_logs(self, task_id, start, end):
        log_of_task_id = self.logs_db[str(task_id)]
        query = {"$and":
            [
                {"_id": {"$gte": start}},
                {"_id": {"$lte": end}}
            ]
        }
        for object in log_of_task_id.find(query):
            tmp_stamp = object['time_stamp']
            tmp_text = object['text']
            logging.info(f'{task_id}\t\t\t\t{tmp_stamp}\t\t\t\t\t\t{tmp_text}')

    def clean_DB(self):
        for collection_name in self.logs_db.list_collection_names():
            self.logs_db[collection_name].delete_many({})
