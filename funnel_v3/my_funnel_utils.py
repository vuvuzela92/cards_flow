import os
import json

def load_api_tokens():
    # Путь к файлу относительно корня проекта
    tokens_path = os.path.join(os.path.dirname(__file__), 'tokens.json')
    with open(tokens_path, 'r', encoding='utf-8') as f:
        return json.load(f)