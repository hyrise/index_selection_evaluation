import pyhdb


class DatabaseConnector:
    def __init__(self, host, port, user, password):
        self.connection = pyhdb.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

        self.cursor = self.connection.cursor()


def main():
    db_conn = DatabaseConnector()

if __name__ == '__main__':
    main()
