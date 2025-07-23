import os

# Setting up proxy configuration to bypass API's restrictions on requests from cloud services
username = os.environ.get("PROXY_USERNAME")
password = os.environ.get("PROXY_PASSWORD")
port = os.environ.get("PROXY_PORT")
proxy = f"http://{username}:{password}@gate.smartproxy.com:{port}"
proxies = {
    "http": proxy,
    "https": proxy
}

