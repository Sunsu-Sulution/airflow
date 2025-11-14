import os
import time
import glob
import pandas as pd
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from util.chrome import Chrome

def _find_newest_file_with_ext(dirpath, exts):
    candidates = []
    for ext in exts:
        candidates += glob.glob(os.path.join(dirpath, f"*{ext}"))
    if not candidates:
        return None
    candidates.sort(key=os.path.getmtime, reverse=True)
    return candidates[0]

def get_food_story_bills_detail(date, wait_for_download=30):
    chrome = Chrome()
    driver = chrome.get_chrome_driver(os.environ.get("DOWLOAD_DIR"))
    selected_date = date.strftime('%Y/%m/%d')

    driver.get(os.environ.get("QUISKSIGHT_URL"))
    driver.maximize_window()

    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="username-input"]'))).send_keys(
        os.environ.get("QUISKSIGHT_USERNAME"))
    driver.find_element(By.XPATH, '//*[@id="username-submit-button"]').click()
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="awsui-input-0"]'))).send_keys(
            os.environ.get("QUISKSIGHT_PASSWORD"))
    except:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="awsui-input-0"]'))).send_keys(
            os.environ.get("QUISKSIGHT_PASSWORD"))
    driver.find_element(By.XPATH, '//*[@id="password-submit-button"]/button').click()
    time.sleep(5)

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[4]/div/header/div[2]/div[1]/button[3]"))).click()
    except:
        elem = driver.find_element(By.XPATH, '//span[text()="Show me more"]')
        driver.execute_script("arguments[0].click()", elem)

    try:
        driver.find_elements(By.XPATH, '//span[text()="Done"]')[0].click()
    except:
        pass

    driver.find_element(By.XPATH,
                        '/html/body/div[4]/div/div[2]/div[1]/div/div/div[2]/div[1]/div[2]/div/div[1]/div/div/div/div[1]/div/span').click()
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,
                                                               '//*[@id="block-b7015f17-2223-44a2-8c8c-50d65fec4bc0_d9dffd9e-3986-4837-b60a-4d34e1654a0a"]/div[2]/div/div[2]/div/div[2]/div[1]/div'))).click()

    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="sheet_control_panel_header"]/div[1]/div/h5[1]/div'))).click()
    time.sleep(5)

    # StartDate
    start_xpath = '//*[@id="block-b7998ade-f882-431a-a7db-64bb342421b8"]/div/div/div[2]/div/input'
    driver.find_element(By.XPATH, start_xpath).click()
    driver.find_element(By.XPATH, '/html/body/div[5]/div[1]').click()
    el = driver.find_element(By.XPATH, start_xpath)
    el.send_keys(Keys.CONTROL, 'a')
    el.send_keys(Keys.DELETE)
    el.send_keys(selected_date)
    el.send_keys(Keys.ENTER)

    # EndDate
    end_xpath = '//*[@id="block-553b20ec-ced5-4b45-8f31-cd5b85a33960"]/div/div/div[2]/div/input'
    driver.find_element(By.XPATH, end_xpath).click()
    driver.find_element(By.XPATH, '/html/body/div[5]/div[1]').click()
    el = driver.find_element(By.XPATH, end_xpath)
    el.send_keys(Keys.CONTROL, 'a')
    el.send_keys(Keys.DELETE)
    el.send_keys(selected_date)
    el.send_keys(Keys.ENTER)

    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH,
        '//*[@id="block-b7015f17-2223-44a2-8c8c-50d65fec4bc0_bdc00ed5-4d1e-4298-ada5-b1d2657b53cb"]'))).click()
    try:
        WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="visual_controls_floating_container"]/div/div/span/button'))).click()
    except:
        elem = driver.find_element(By.XPATH, '//*[@id="visual_controls_floating_container"]/div/div/span/button')
        driver.execute_script("arguments[0].click()", elem)

    time.sleep(1)
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Export to CSV')]"))).click()

    download_dir = (
        os.environ.get("DOWLOAD_DIR")
    )

    if not os.path.isdir(download_dir):
        try:
            os.makedirs(download_dir, exist_ok=True)
        except Exception:
            download_dir = "/tmp"

    start_time = time.time()
    initial = _find_newest_file_with_ext(download_dir, ['.csv'])
    initial_mtime = os.path.getmtime(initial) if initial else 0

    timeout = wait_for_download
    while time.time() - start_time < timeout:
        newest = _find_newest_file_with_ext(download_dir, ['.csv'])
        if newest:
            try:
                mtime = os.path.getmtime(newest)
            except Exception:
                mtime = 0
            if mtime >= initial_mtime and mtime >= start_time - 1:
                size1 = os.path.getsize(newest)
                time.sleep(1)
                size2 = os.path.getsize(newest)
                if size1 == size2 and size1 > 0:
                    break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"No CSV/ZIP was downloaded into {download_dir} within {wait_for_download}s")

    df = None
    try:
        _, ext = os.path.splitext(newest.lower())
        if ext == ".zip":
            import zipfile
            with zipfile.ZipFile(newest) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                if not csv_names:
                    raise RuntimeError("zip archive contains no csv files")
                with zf.open(csv_names[0]) as fh:
                    df = pd.read_csv(fh)
        else:
            df = pd.read_csv(newest)
    except Exception as e:
        raise RuntimeError(f"Failed to read downloaded file {newest}: {e}")
    finally:
        try:
            os.remove(newest)
        except Exception:
            pass

    return df

def get_food_story_promotions(date, wait_for_download=30):
    chrome = Chrome()
    driver = chrome.get_chrome_driver(os.environ.get("DOWLOAD_DIR"))
    selected_date = date.strftime('%Y/%m/%d')

    driver.get(os.environ.get("QUISKSIGHT_URL"))
    driver.maximize_window()

    WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="username-input"]'))).send_keys(
        os.environ.get("QUISKSIGHT_USERNAME"))
    driver.find_element(By.XPATH, '//*[@id="username-submit-button"]').click()
    try:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="awsui-input-0"]'))).send_keys(
            os.environ.get("QUISKSIGHT_PASSWORD"))
    except:
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="awsui-input-0"]'))).send_keys(
            os.environ.get("QUISKSIGHT_PASSWORD"))
    driver.find_element(By.XPATH, '//*[@id="password-submit-button"]/button').click()
    time.sleep(5)

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[4]/div/header/div[2]/div[1]/button[3]"))).click()
    except:
        elem = driver.find_element(By.XPATH, '//span[text()="Show me more"]')
        driver.execute_script("arguments[0].click()", elem)

    try:
        driver.find_elements(By.XPATH, '//span[text()="Done"]')[0].click()
    except:
        pass

    driver.find_element(By.XPATH,
                        '/html/body/div[4]/div/div[2]/div[1]/div/div/div[2]/div[1]/div[2]/div/div[1]/div/div/div/div[1]/div/span').click()
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,
                                                               '//*[@id="block-b7015f17-2223-44a2-8c8c-50d65fec4bc0_d9dffd9e-3986-4837-b60a-4d34e1654a0a"]/div[2]/div/div[2]/div/div[2]/div[1]/div'))).click()

    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="sheet_control_panel_header"]/div[1]/div/h5[1]/div'))).click()
    time.sleep(5)

    # StartDate
    start_xpath = '//*[@id="block-b7998ade-f882-431a-a7db-64bb342421b8"]/div/div/div[2]/div/input'
    driver.find_element(By.XPATH, start_xpath).click()
    driver.find_element(By.XPATH, '/html/body/div[5]/div[1]').click()
    el = driver.find_element(By.XPATH, start_xpath)
    el.send_keys(Keys.CONTROL, 'a')
    el.send_keys(Keys.DELETE)
    el.send_keys(selected_date)
    el.send_keys(Keys.ENTER)

    # EndDate
    end_xpath = '//*[@id="block-553b20ec-ced5-4b45-8f31-cd5b85a33960"]/div/div/div[2]/div/input'
    driver.find_element(By.XPATH, end_xpath).click()
    driver.find_element(By.XPATH, '/html/body/div[5]/div[1]').click()
    el = driver.find_element(By.XPATH, end_xpath)
    el.send_keys(Keys.CONTROL, 'a')
    el.send_keys(Keys.DELETE)
    el.send_keys(selected_date)
    el.send_keys(Keys.ENTER)

    driver.execute_script("window.scrollTo(0, (document.body.scrollHeight)/4);")
    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH,
                                                               '//*[@id="block-b7015f17-2223-44a2-8c8c-50d65fec4bc0_d9dffd9e-3986-4837-b60a-4d34e1654a0a"]'))).click()

    try:
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable(
            (By.XPATH, '//*[@id="visual_controls_floating_container"]/div/div/span/button'))).click()
    except:
        elem = driver.find_element(By.XPATH, '//*[@id="visual_controls_floating_container"]/div/div/span/button')
        driver.execute_script("arguments[0].click()", elem)

    time.sleep(1)
    WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Export to CSV')]"))).click()

    download_dir = (
        os.environ.get("DOWLOAD_DIR")
    )

    if not os.path.isdir(download_dir):
        try:
            os.makedirs(download_dir, exist_ok=True)
        except Exception:
            download_dir = "/tmp"

    start_time = time.time()
    initial = _find_newest_file_with_ext(download_dir, ['.csv'])
    initial_mtime = os.path.getmtime(initial) if initial else 0

    timeout = wait_for_download
    while time.time() - start_time < timeout:
        newest = _find_newest_file_with_ext(download_dir, ['.csv'])
        if newest:
            try:
                mtime = os.path.getmtime(newest)
            except Exception:
                mtime = 0
            if mtime >= initial_mtime and mtime >= start_time - 1:
                size1 = os.path.getsize(newest)
                time.sleep(1)
                size2 = os.path.getsize(newest)
                if size1 == size2 and size1 > 0:
                    break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"No CSV/ZIP was downloaded into {download_dir} within {wait_for_download}s")

    df = None
    try:
        _, ext = os.path.splitext(newest.lower())
        if ext == ".zip":
            import zipfile
            with zipfile.ZipFile(newest) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
                if not csv_names:
                    raise RuntimeError("zip archive contains no csv files")
                with zf.open(csv_names[0]) as fh:
                    df = pd.read_csv(fh)
        else:
            df = pd.read_csv(newest)
    except Exception as e:
        raise RuntimeError(f"Failed to read downloaded file {newest}: {e}")
    finally:
        try:
            os.remove(newest)
        except Exception:
            pass

    return df
