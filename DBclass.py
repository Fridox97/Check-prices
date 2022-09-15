class DbInfo:
    def __init__(self, hostname, username, password, mainDatabase, sql_port, host, user, ssh_port, pem_path):
        self.sql_hostname = hostname
        self.sql_username = username
        self.sql_password = password
        self.sql_main_database = mainDatabase
        self.sql_port = sql_port
        self.ssh_host = host
        self.ssh_user = user
        self.ssh_port = ssh_port
        self.pem = pem_path