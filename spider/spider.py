from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import csv
import re


class Spider(object):
    def __init__(self, job_type, city, page):
        self.job_type = job_type
        self.city = city
        self.page = page
        self.spider_url = 'https://www.lagou.com/wn/jobs?fromSearch=true&kd=%s&city=%s&pn=%s'
        self.driver = None

    def start_browser(self):
        """连接到已启动的 Chrome 实例"""
        options = webdriver.ChromeOptions()
        options.debugger_address = "localhost:9222"  # 连接到端口 9222
        try:
            # 不需要指定 service，因为我们连接已有实例
            browser = webdriver.Chrome(options=options)
            print("成功连接到现有 Chrome 实例")
            return browser
        except Exception as e:
            print(f"连接 Chrome 失败: {e}")
            print("请确保 Chrome 已通过 '--remote-debugging-port=9222' 启动")
            return None

    def extract_job_data(self, job, index):
        """提取单个职位的数据"""
        try:
            wait = WebDriverWait(self.driver, 5)
            title = wait.until(EC.presence_of_element_located((By.XPATH,
                                                               f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="p-top__1F7CL"]/a'))).text
            company_title = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                       f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="company-name__2-SjF"]/a'))).text
            salary = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="p-bom__JlNur"]/span'))).text
            salary_nums = re.findall(r'\d+', salary)
            min_salary = int(salary_nums[0]) * 1000 if salary_nums else 0
            max_salary = int(salary_nums[1]) * 1000 if len(salary_nums) > 1 else min_salary

            double = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="p-bom__JlNur"]'))).get_attribute(
                'textContent').split('/')
            work_experience = double[0].split('k')[2].strip()
            education = double[1].strip() if len(double) > 1 else '未知'

            try:
                total_tag = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                       f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="company__2EsC8"]/div[@class="industry__1HBkr"]'))).text
                company_people = re.findall(r'\d+', total_tag)
                company_people = '-'.join(company_people) if company_people else '未知'
            except:
                total_tag = '无'
                company_people = '未知'

            tag_list = self.driver.find_elements(By.XPATH,
                                                 f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="ir___QwEG"]/span')
            work_tag = '/'.join([tag.text for tag in tag_list]) if tag_list else '无'

            try:
                welfare = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                     f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="il__3lk85"]'))).text.replace(
                    '“', '').replace('”', '')
            except:
                welfare = '无'

            img_src = wait.until(EC.presence_of_element_located((By.XPATH,
                                                                 f'//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"][{index + 1}]//div[@class="com-logo__1QOwC"]/img'))).get_attribute(
                'src')

            return [self.job_type, title, company_title, min_salary, max_salary, work_experience, education, total_tag,
                    company_people, work_tag, welfare, img_src, self.city]
        except Exception as e:
            print(f"提取第 {index + 1} 个职位数据失败: {e}")
            return None

    def main(self, max_page):
        if self.page > max_page:
            return

        if not self.driver:
            self.driver = self.start_browser()
            if not self.driver:
                return

        url = self.spider_url % (self.job_type, self.city, self.page)
        print(f"正在爬取第 {self.page} 页: {url}")

        retries = 3
        for attempt in range(retries):
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.ID, "jobList"))
                )
                break
            except Exception as e:
                print(f"尝试 {attempt + 1}/{retries} 失败: {e}")
                if attempt == retries - 1:
                    print("达到最大重试次数，放弃该页")
                    print("页面源码：", self.driver.page_source[:1000])
                    self.driver.quit()
                    return
                time.sleep(5)

        job_list = self.driver.find_elements(By.XPATH,
                                             '//div[@id="jobList"]/div[@class="list__YibNq"]/div[@class="item__10RTO"]')
        print(f"找到 {len(job_list)} 个职位")

        for index in range(len(job_list)):
            data = self.extract_job_data(None, index)
            if data:
                print(
                    f"{data[1]}, {data[2]}, {data[3]}, {data[4]}, {data[5]}, {data[6]}, {data[7]}, {data[8]}, {data[9]}, {data[10]}, {data[11]}")
                self.save_data(data)

        self.page += 1
        time.sleep(2)
        self.main(max_page)

    def save_data(self, data):
        with open('../spark/jobData.csv', 'a', newline='', encoding='utf-8') as wf:
            writer = csv.writer(wf)
            writer.writerow(data)

    def init(self):
        if not os.path.exists('../spark/jobData.csv'):
            with open('../spark/jobData.csv', 'w', encoding='utf-8', newline='') as wf:
                writer = csv.writer(wf)
                writer.writerow(
                    ['type', 'title', 'companyTitle', 'minSalary', 'maxSalary', 'workExperience', 'education',
                     'totalTag', 'companyPeople', 'workTag', 'welfare', 'imgSrc', 'city'])

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None


if __name__ == '__main__':
    city_list = ['北京', '上海', '广州', '深圳', '杭州', '苏州', '成都', '南京', '武汉', '长沙']
    type_list = ['java', 'web前端', 'C语言', 'php开发', '数据分析师', '软件测试', 'IT运维', '微信小程序', '.NET']

    for city in city_list:
        for job_type in type_list:
            spider_obj = Spider(job_type, city, 1)
            spider_obj.init()
            spider_obj.main(5)
            spider_obj.close()