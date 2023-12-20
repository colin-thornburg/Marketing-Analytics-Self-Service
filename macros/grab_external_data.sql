{% macro grab_external_data() %}
CREATE OR REPLACE FUNCTION google_translate_python(sentence STRING, language STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'get_translation'
EXTERNAL_ACCESS_INTEGRATIONS = (google_apis_access_integration)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('cred' = oauth_token )
AS
$$
import _snowflake
import requests
import json
session = requests.Session()
def get_translation(sentence, language):
  token = _snowflake.get_oauth_access_token('cred')
  url = "https://translation.googleapis.com/language/translate/v2"
  data = {'q': sentence,'target': language}
  response = session.post(url, json = data, headers = {"Authorization": "Bearer " + token})
  return response.json()['data']['translations'][0]['translatedText']
$$;
{% endmacro %}