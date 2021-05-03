from pymongo import MongoClient


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
        print(f'### generate log info of task id: {task_id}###')
        for object in log_of_task_id.find(query):
            t_stamp = object['time_stamp']
            t_text = object['text']
            print(f'time_stamp - {t_stamp}, text - {t_text}')

    def clean_logs(self):
        list_collections = self.logs_db.list_collections()
        self.logs_db["1"].delete_many({})
