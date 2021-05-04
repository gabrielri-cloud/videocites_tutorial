from LogServer import LogServer


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    log_server = LogServer()
    log_server.add_log(task_id=1, time_stamp=1, text="this is text1")
    log_server.add_log(task_id=1, time_stamp=12, text="this is text12")
    log_server.add_log(task_id=1, time_stamp=5, text="this is text5")
    log_server.add_log(task_id=1, time_stamp=2, text="this is text2")
    log_server.add_log(task_id=22, time_stamp=5, text="this is text5")
    log_server.add_log(task_id=22, time_stamp=2, text="this is text2")
    log_server.generate_logs(task_id=1, start=0, end=10)
    log_server.generate_logs(task_id=22, start=1, end=3)
    log_server.clean_DB()

