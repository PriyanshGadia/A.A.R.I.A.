# mistral_client.py
from mistralai import Mistral

class MistralLLMClient:
    def __init__(self, api_key: str, model: str = "mistral-large-latest"):
        self.client = Mistral(api_key=api_key)
        self.model = model

    def generate(self, messages: list, max_tokens: int = 400, temperature: float = 0.2) -> str:
        chat_response = self.client.chat.complete(
            model=self.model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature
        )
        return chat_response.choices[0].message.content
