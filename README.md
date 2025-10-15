# DDPipe
Datadog→Pandas/DuckDB bridge

API for interfacing with Datadog in a Pythonic way. Turning messy JSON into structured tables - easy integration with Pandas DataFrames (DuckDB coming soon!).

To run the project (locally on Windows):

1. Run `git clone https://github.com/dzyunkin1/DDPipe.git` and `cd` into the downloaded directory.
2. Activate the virtual environment and install dependencies:
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
3. Create a Datadog account and copy your API key, App Key, and region into a `.env` file. Make sure that your keys have appropriate permissions for what you are trying to monitor. Expected layout of the `.env` file:
```
DD_API_KEY='your_api_key_here'
DD_APP_KEY='your_app_key_here'
DD_SITE='your_region_here (eg. us5.datadoghq.com)'
```
