HOST = "127.0.0.1"
PORT = 3306
USER = "root"
PASSWORD = ""
DATABASE_NAME = "domains_checker"
NEED_UPDATE_SECOUNDS = 3600 #Обновление нужно каждый час. 60секунд на 60 минут
CHECK_TIME = 60 #интервалы проверки. проверять нуждаемость в обновлении каждую минуту

FILENAME = "mail.txt"
LIST_DNS_SERVERS = [
    '208.67.222.222', '208.67.220.220', '1.1.1.1', '1.0.0.1', '8.8.8.8',
    '8.8.4.4', '9.9.9.9', '149.112.112.112', '8.26.56.26', '9.9.9.9',
    '77.88.8.8', '94.140.14.14', '93.115.24.204', '92.223.109.31',
    '198.153.192.1', '198.153.194.1', '64.6.65.6', '64.6.64.6',
    '156.154.71.5', '156.154.70.5', '4.2.2.4', '4.2.2.3', '216.146.35.35',
    '216.146.36.36', '4.2.2.2', '4.2.2.1'
]
