from selenium import webdriver
from selenium.webdriver.chrome.service import Service

# # 指定 ChromeDriver 路径
# service = Service(executable_path="G:\基于spark的招聘数据分析预测推荐系统\djangoProject\spider\chromedriver.exe")
# driver = webdriver.Chrome(service=service)
#
# # 打开网页测试
# driver.get("https://www.google.com")
# print(driver.title)
# driver.quit()
from pyhive import hive
conn = hive.Connection(host='node1', port=10000, username='root', auth='NOSASL')
print("连接成功")
conn.close()