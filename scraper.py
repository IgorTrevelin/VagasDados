import re
import time
import threading
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from chromedriver_py import binary_path
from joblib import Parallel, delayed
from bs4 import BeautifulSoup

writer = pd.ExcelWriter("dados/vagas.xlsx", engine="auto")

"""
    Web Scraping de vagas publicadas no LinkedIn
"""


def get_linkedin_jobs(n_jobs=1000):
    """
    This code was based on the article from https://maoviola.medium.com/a-complete-guide-to-web-scraping-linkedin-job-postings-ad290fcaa97f
    """
    url = "https://www.linkedin.com/jobs/search?keywords=dados&location=Brazil&geoId=106057199&trk=public_jobs_jobs-search-bar_search-submit&currentJobId=3407927520&position=5&pageNum=0"

    service_object = Service(binary_path)
    driver = webdriver.Chrome(service=service_object)

    driver.get(url)

    no_of_jobs = int(
        driver.find_element(By.CSS_SELECTOR, "h1 > span")
        .get_attribute("innerText")
        .replace(".", "")
        .replace("+", "")
    )

    n_jobs = min(n_jobs, no_of_jobs)

    last_len_jobs = 0
    while True:
        job_lists = driver.find_element(By.CLASS_NAME, "jobs-search__results-list")
        jobs = job_lists.find_elements(By.TAG_NAME, "li")

        if last_len_jobs == len(jobs):
            driver.execute_script("window.scrollTo(0, 0);")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(30)
            continue

        if len(jobs) >= n_jobs:
            break

        last_len_jobs = len(jobs)

        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            load_more_btn = driver.find_element(
                By.CSS_SELECTOR, ".infinite-scroller__show-more-button"
            )

            load_more_btn.click()
        except:
            pass
        finally:
            time.sleep(10)

    job_lists = driver.find_element(By.CLASS_NAME, "jobs-search__results-list")
    jobs = job_lists.find_elements(By.TAG_NAME, "li")

    job_title = []
    company_name = []
    location = []
    date = []
    job_link = []
    job_description = []
    seniority = []
    emp_type = []
    job_func = []
    industries = []

    for job in jobs:
        try:
            job_title0 = (
                job.find_element(By.CSS_SELECTOR, "h3")
                .get_attribute("innerText")
                .strip()
            )
            company_name0 = (
                job.find_element(By.CSS_SELECTOR, "h4")
                .get_attribute("innerText")
                .strip()
            )
            location0 = job.find_element(
                By.CSS_SELECTOR, '[class="job-search-card__location"]'
            ).get_attribute("innerText")
            date0 = job.find_element(By.CSS_SELECTOR, "div>div>time").get_attribute(
                "datetime"
            )
            job_link0 = job.find_element(By.CSS_SELECTOR, "a").get_attribute("href")

            job.find_element(By.TAG_NAME, "a").click()

            details = driver.find_element(
                By.CSS_SELECTOR, ".two-pane-serp-page__detail-view"
            )

            jd0 = (
                details.find_element(By.CSS_SELECTOR, ".show-more-less-html__markup")
                .get_attribute("innerText")
                .strip()
            )

            criteria_list = details.find_element(
                By.CSS_SELECTOR, ".description__job-criteria-list"
            )

            seniority0 = (
                criteria_list.find_element(By.XPATH, "//li[1]/span")
                .get_attribute("innerText")
                .strip()
            )

            emp_type0 = (
                criteria_list.find_element(By.XPATH, "//li[2]/span")
                .get_attribute("innerText")
                .strip()
            )

            job_func0 = (
                criteria_list.find_element(By.XPATH, "//li[3]/span")
                .get_attribute("innerText")
                .strip()
            )

            industries0 = (
                criteria_list.find_element(By.XPATH, "//li[4]/span")
                .get_attribute("innerText")
                .strip()
            )

            date.append(date0)
            company_name.append(company_name0)
            job_title.append(job_title0)
            location.append(location0)
            job_description.append(jd0)
            seniority.append(seniority0)
            emp_type.append(emp_type0)
            job_func.append(job_func0)
            industries.append(industries0)
            job_link.append(job_link0)
        except:
            pass
        finally:
            time.sleep(5)

        job_data = pd.DataFrame(
            {
                "Data": date[:n_jobs],
                "Empresa": company_name[:n_jobs],
                "Título": job_title[:n_jobs],
                "Localização": location[:n_jobs],
                "Descrição": job_description[:n_jobs],
                "Nível": seniority[:n_jobs],
                "Tipo": emp_type[:n_jobs],
                "Função": job_func[:n_jobs],
                "Indústria": industries[:n_jobs],
                "Link": job_link[:n_jobs],
            }
        )

        job_data["Descrição"] = job_data["Descrição"].str.replace("\n", " ")
        job_data.to_excel(writer, "LinkedIn", index=False)

    driver.quit()


"""
    Web Scraping de vagas publicadas no site vagas.com.br
"""


def get_vagascombr_jobs(n_jobs=1000):
    url = "https://www.vagas.com.br/vagas-de-dados"
    service_object = Service(binary_path)
    driver = webdriver.Chrome(service=service_object)

    driver.get(url)

    no_of_jobs = driver.find_element(By.TAG_NAME, "h1").get_attribute("innerText")
    s = re.search("^(\d+) .*", no_of_jobs)
    no_of_jobs = int(s.group(1))

    n_jobs = min(n_jobs, no_of_jobs)

    while True:
        jobs = driver.find_elements(By.CSS_SELECTOR, "li.vaga")
        print(len(jobs))
        if len(jobs) >= n_jobs:
            jobs = jobs[:n_jobs]
            break

        try:
            load_more = driver.find_element(By.ID, "maisVagas")
            load_more.click()
        except:
            pass
        finally:
            time.sleep(5)

    links = [
        job.find_element(By.CLASS_NAME, "link-detalhes-vaga").get_attribute("href")
        for job in jobs
    ]

    driver.quit()

    def get_job_data(url):
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            date = soup.find("li", {"class": "job-breadcrumb__item--published"}).text
            date = re.search("Publicada em (.+)", date).group(1)

            title = soup.find(
                "h1", {"class": "job-shortdescription__title"}
            ).text.strip()
            company = soup.find(
                "h2", {"class": "job-shortdescription__company"}
            ).text.strip()
            description = soup.find(
                "div", {"class": "job-description__text"}
            ).text.strip()
            location = soup.find("span", {"class": "info-localizacao"}).text.strip()
            seniority = soup.find(
                "span", {"class": "job-hierarchylist__item--level"}
            ).text.strip()

            return [date, title, company, description, location, seniority, url]
        except:
            return None

    jobs_data = Parallel(n_jobs=5)(delayed(get_job_data)(link) for link in links)
    jobs_data = list(filter(lambda x: x is not None, jobs_data))

    job_data = pd.DataFrame(
        jobs_data,
        columns=[
            "Data",
            "Título",
            "Empresa",
            "Descrição",
            "Localização",
            "Nível",
            "Link",
        ],
    )

    job_data["Descrição"] = job_data["Descrição"].str.replace("\n", " ")
    job_data.to_excel(writer, "vagas.com.br", index=False)


def main():
    """
    Executando jobs de web scraping em paralelo
    """
    jobs = [get_linkedin_jobs, get_vagascombr_jobs]

    threads = list(map(lambda job: threading.Thread(target=job, daemon=True), jobs))

    for thread in threads:
        thread.start()

    while True:
        alive = len(list(filter(lambda thread: thread.is_alive(), threads)))

        if alive == 0:
            return

        time.sleep(5)


if __name__ == "__main__":
    main()
    writer.close()
    exit(1)
