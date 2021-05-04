from pymongo import MongoClient
import logging


class LogServer:
    def __init__(self, data_base_name, print_format):
        client = \
            MongoClient('mongodb+srv://new_user:<password>@gabrieldb.lrteu.mongodb.net/myFirstDatabase'
                        '?retryWrites=true&w=majority',
                        username='new_user',
                        password='new_user',
                        tls=True,
                        tlsAllowInvalidCertificates=True)
        self.print_format = print_format
        self.logs_db = client.get_database(data_base_name)
        logging.basicConfig(filename='LogServer.log', level=logging.INFO, filemode='w', format='%(message)s')
        try:
            logging.info(self.print_format.format('task_id', 'time stamp', 'text'))
        except:
            logging.info('Error - format not valid')


    def add_log(self, task_id, time_stamp, text):
        entry = {
            'time_stamp': time_stamp,
            'text': text
        }
        try:
            self.logs_db[str(task_id)].insert_one(entry)
        except:
            print(f'task_id: {task_id}, time_stamp: {time_stamp} - already on the mongoDB')

    def generate_logs(self, task_id, start, end):
        log_of_task_id = self.logs_db[str(task_id)]
        query = {"time_stamp": {"$gte": start, "$lte": end}}

        for document in log_of_task_id.find(query):
            tmp_stamp = document.get('time_stamp')
            tmp_text = document.get('text')
            try:
                logging.info(self.print_format.format(task_id, tmp_stamp, tmp_text))
            except:
                logging.info('Error - format not valid')

    # danger - not to use permanent
    def clean_DB(self):
        for collection_name in self.logs_db.list_collection_names():
            self.logs_db[collection_name].delete_many({})
