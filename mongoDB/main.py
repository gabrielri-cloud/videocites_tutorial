from LogServer import LogServer

from datetime import datetime


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    log_server = LogServer(data_base_name='logs', print_format='{}\t\t\t\t\t{}\t\t\t\t\t{}')
    log_server.add_log(task_id=1, time_stamp=1, text="this is text1")
    log_server.add_log(task_id=1, time_stamp=12, text="this is text12")
    log_server.add_log(task_id=1, time_stamp=5, text="this is text5")
    log_server.add_log(task_id=1, time_stamp=2, text="this is text2")
    log_server.add_log(task_id=22, time_stamp=5, text="this is text5")
    log_server.add_log(task_id=22, time_stamp=2, text="this is text2")
    log_server.generate_logs(task_id=1, start=0, end=10)
    log_server.generate_logs(task_id=22, start=1, end=3)
    log_server.generate_logs(task_id=33, start=1, end=3)

    timestamp1 = 1545730073
    timestamp2 = 1545730080
    timestamp_mid = 1545730075
    dt_object1 = datetime.fromtimestamp(timestamp1)
    dt_object2 = datetime.fromtimestamp(timestamp2)
    dt_object_mid = datetime.fromtimestamp(timestamp_mid)
    log_server.add_log(task_id=5, time_stamp=dt_object1, text="this is text timestamp_in")
    log_server.add_log(task_id=5, time_stamp=dt_object2, text="this is timestamp_not_in")
    log_server.generate_logs(task_id=5, start=dt_object1, end=dt_object_mid)

    log_server.clean_DB()

