import json

# Функция для загрузки API токенов из файла tokens.json
def load_api_tokens():
    with open(r'C:\Users\123\Desktop\cards_flow_git\tokens.json', encoding= 'utf-8') as f:
        tokens = json.load(f)
        return tokens