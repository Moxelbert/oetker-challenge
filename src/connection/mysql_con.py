class MySql:
    def __init__(self, user, pw):
        self.properties = {
            'user': user,
            'password': pw,
            'driver': 'com.mysql.cj.jdbc.Driver'
        }

    def write_to_db(self, dataframe, url, table, mode):
        dataframe.write.jdbc(url='jdbc:mysql://' + url,
                             table=table,
                             mode=mode,
                             properties=self.properties)
