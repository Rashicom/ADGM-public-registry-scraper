import json
import requests
from helper import aura_context_data, get_last_row_date, get_company_ids
from datetime import datetime, timedelta
from calendar import calendar, monthrange