import requests
from bs4 import BeautifulSoup
import concurrent.futures
import asyncio
import logging
import csv


logging.basicConfig(level=logging.INFO)

event_loop = asyncio.get_event_loop()
executor = concurrent.futures.ThreadPoolExecutor(10)
MAX_CONCURRENT_REQUESTS = 10
csv_data = []




async def async_post_request(url, data):
    """
    Creates Async POST URL Request to make information fetching faster
    """

    logging.info(f"Fetching {url} with data {data}")
    return await event_loop.run_in_executor(executor, requests.post, url, data)


async def async_get_request(url, app_name):
    """
    Creates Async GET URL Request to make information fetching faster
    """
    logging.info(f"Fetching Information for {app_name.encode('unicode-escape').decode('utf-8')}")
    html_content = await event_loop.run_in_executor(executor, requests.get, url)
    return {"app_name": app_name, "response": html_content}


async def write_to_csv(data,file_name):
    """
    Write CSV Data to File
    :param data:
    :return:
    """
    with open(file_name, mode="a") as output_file:
        employee_writer = csv.writer(
            output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        employee_writer.writerow(data)


async def get_all_main_urls(collection_url):
    """
    Generates All Main Page URLS
    :return:
    """
    post_request_params = [
        {"start": 0, "num": 120},  # Total 120
        {"start": 120, "num": 120},  # Total 240
        {"start": 240, "num": 120},  # Total 360
        {"start": 360, "num": 120},  # Total 480
        {"start": 480, "num": 60}  # Total 540
    ]

    main_url_requests = []
    for each_data in post_request_params:
        main_url_requests.append(
            async_post_request(
                collection_url, each_data
            )
        )
    return main_url_requests


def find_all_app_links(htmlcontent):
    """
    Fetches all app links from the downloaded HTML Page
    :return:
    """
    soup = BeautifulSoup(htmlcontent, "html.parser")

    all_anchors = soup.find_all("a")

    url_data = []
    for each_anchor in all_anchors:
        if (
            (each_anchor["href"].find("/store/apps/details?id=") != -1)
            and not each_anchor.get("aria-hidden", False)
            and each_anchor.get("aria-label", None)
        ):

            url_data.append(
                {
                    "app_name": each_anchor.get("aria-label").strip(),
                    "app_url": f"https://play.google.com{each_anchor['href']}",
                }
            )
    return url_data


async def write_developer_info_to_csv(html_response,file_name):
    """
    Writes the developer information from html data to csv
    """
    content = html_response["response"].content
    app_name = html_response["app_name"]
    logging.info(f"Processing Data for {app_name.encode('unicode-escape').decode('utf-8')}")

    soup = BeautifulSoup(content, "html.parser")
    mailtos = soup.select("a[href^=mailto]")
    email_address = mailtos[0].contents[0]
    await write_to_csv([app_name.encode('unicode-escape').decode('utf-8'), email_address], file_name)


async def co_ordinate_operations(collection_url,file_name):
    """
    Co ordinate all the tasks required for performing the operation
    """
    main_urls = await get_all_main_urls(collection_url)
    all_data = await asyncio.gather(*main_urls)
    logging.info("Completed Fetching All Main URLS")
    all_app_links = []
    for each_page_set in all_data:
        all_app_links = all_app_links + find_all_app_links(each_page_set.content)
    batched_url_requests = []
    responses = []

    for each_app_info in all_app_links:
        batched_url_requests.append(
            async_get_request(each_app_info.get("app_url"), each_app_info.get("app_name"))
        )
        if len(batched_url_requests) >= MAX_CONCURRENT_REQUESTS:
            individual_response = await asyncio.gather(*batched_url_requests)
            responses = responses + individual_response
            batched_url_requests = []

    if len(batched_url_requests):
        individual_response = await asyncio.gather(*batched_url_requests)
        responses = responses + individual_response

    batched_file_writes = []

    for each_response in responses:
        batched_file_writes.append(write_developer_info_to_csv(each_response,file_name))
        if len(batched_file_writes) > MAX_CONCURRENT_REQUESTS:
            await asyncio.gather(*batched_file_writes)
            batched_file_writes = []

    await asyncio.gather(*batched_file_writes)


def write_headers(file_name):
    """
    Writes Heading to the CSV File
    :return:
    """

    with open(file_name, mode="w") as output_file:
        employee_writer = csv.writer(
            output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        employee_writer.writerow(["APP NAME", "DEVELOPER EMAIL"])


if __name__ == "__main__":
    logging.info("Starting Main")
    print("Enter PlayStore Collection URL " ,end=": ")
    collection_url = input()
    print("Enter File Name", end=": ")
    file_name = input()
    file_name = file_name+'.csv'
    write_headers(file_name)
    responses = event_loop.run_until_complete(co_ordinate_operations(collection_url,file_name))
