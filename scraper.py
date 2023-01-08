import os
import time
import threading
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from chromedriver_py import binary_path

dumpfolder = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")


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

        if len(job_link) % 5 == 0:
            job_data = pd.DataFrame(
                {
                    "Date": date[:n_jobs],
                    "Company": company_name[:n_jobs],
                    "Title": job_title[:n_jobs],
                    "Location": location[:n_jobs],
                    "Description": job_description[:n_jobs],
                    "Level": seniority[:n_jobs],
                    "Type": emp_type[:n_jobs],
                    "Function": job_func[:n_jobs],
                    "Industry": industries[:n_jobs],
                    "Link": job_link[:n_jobs],
                }
            )

            job_data["Description"] = job_data["Description"].str.replace("\n", " ")
            job_data.to_excel(os.path.join(dumpfolder, "linkedin.xlsx"), index=False)

    driver.quit()


def main():
    jobs = [get_linkedin_jobs]

    threads = list(map(lambda job: threading.Thread(target=job, daemon=True), jobs))

    for thread in threads:
        thread.start()

    while True:
        alive = len(list(filter(lambda thread: thread.is_alive(), threads)))

        if alive == 0:
            exit(1)

        time.sleep(5)


if __name__ == "__main__":
    # main()
    get_linkedin_jobs()
