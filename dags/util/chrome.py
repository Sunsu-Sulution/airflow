from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class Chrome:

    def get_chrome_driver(self, download_dir):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1000')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        options.binary_location = "/usr/bin/chromium"
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)

        driver_service = Service("/usr/bin/chromedriver")
        return webdriver.Chrome(service=driver_service, options=options)
