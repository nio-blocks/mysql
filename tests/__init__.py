def mysql_running(host, port, user, password):
    try:
        import pymysql
    except:
        return False

    try:
        connection = pymysql.connect(host=host,
                                     port=port,
                                     user=user,
                                     passwd=password)
        connection.close()
        return True
    except:
        return False
