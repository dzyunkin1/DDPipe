import os
from dotenv import load_dotenv


def load_env():
    load_dotenv()
    config = {
        "api_key": os.getenv("DD_API_KEY"),
        "app_key": os.getenv("DD_APP_KEY"),
        "site": os.getenv("DD_SITE", "datadoghq.com"),
        "debug": os.getenv("DD_DEBUG", "false").lower() == "true",
    }
    return config
