from pyhive import hive

# 修改连接参数，添加auth='NOSASL'
conn = hive.Connection(host='node1', port=10000, username='root')
cursor = conn.cursor()

def query_hive(sql, params=None, type='no_select'):
    if params is None:
        params = []
    params = tuple(params)
    cursor.execute(sql, params)
    if type != 'no_select':
        data_list = cursor.fetchall()
        conn.commit()
        return data_list
    else:
        conn.commit()
        return '数据库语句执行成功'

# 查看可用的表
print("可用表:")
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print(tables)

# 查看所使用的数据库
cursor.execute("SELECT current_database()")
current_db = cursor.fetchone()
print(f"当前数据库: {current_db}")

# 尝试查询表
data = query_hive('SELECT * FROM jobData', [], 'select')[:1]
print(data)